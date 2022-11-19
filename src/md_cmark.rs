mod split_md;

use anyhow::{Context, Result};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use tracing::debug;

use cached::proc_macro::cached;

use serde::Serialize;
use serde_tuple::Serialize_tuple;

use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use lazy_static::lazy_static;
use regex::{Regex, Replacer};

use bytes::Bytes;

#[derive(Serialize_tuple, Clone)]
struct Htmlblock {
    hash: u64,
    html: String,
    #[serde(skip_serializing)]
    citeblocks: String,
}

#[cached(result = true, size = 8192, key = "u64", convert = "{hash}")]
async fn mdblock2htmlblock(md_block: &str, hash: u64, cwd: &Path) -> Result<Htmlblock> {
    let mut cmd = Command::new("pandoc");
    cmd.current_dir(cwd)
        .arg("--from")
        .arg("markdown+emoji")
        .arg("--to")
        .arg("html5")
        .arg("--katex");

    cmd.stdout(std::process::Stdio::piped());
    cmd.stdin(std::process::Stdio::piped());

    let mut child = cmd.spawn()?;
    let mut stdin = child.stdin.take().context("stdin take failed")?;

    stdin.write_all(md_block.as_bytes()).await?;

    // Send EOF to child
    drop(stdin);

    let out = child.wait_with_output().await?;
    let mut out = String::from_utf8(out.stdout)?;

    let fragment = scraper::Html::parse_fragment(&out);
    lazy_static! {
        static ref CITE_SELECTOR: scraper::Selector =
            scraper::Selector::parse("span.citation").unwrap();
    }

    let citeblocks: String = fragment
        .select(&CITE_SELECTOR)
        .map(|element| element.text().next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n\n");

    // TODO: replace URL_REGEX thing? Now that we anyway parse the html?
    // Replace relative local URLs
    // Alternative is fancy_regex which supports negative backtracking.
    // But this here is faster for the common case where there are only few local links.
    lazy_static! {
        static ref URL_REGEX: Regex = Regex::new(r#"(href|src)=['"](.+?)['"]"#).unwrap();
        static ref URL_REGEX_EXCLUDE_PREFIX: Regex =
            Regex::new(r##"^/|https://|http://|\#"##).unwrap();
    }
    let captures: Vec<regex::Captures> = URL_REGEX
        .captures_iter(&out)
        .filter(|c| !URL_REGEX_EXCLUDE_PREFIX.is_match(c.get(2).unwrap().as_str()))
        .collect();
    if !captures.is_empty() {
        let mut rep = format!(
            r#"$1="file://{}/$2" onclick="return localLinkClickEvent(this);""#,
            cwd.to_str().context("cwd not a string")?
        );

        let mut new = String::with_capacity(out.len());
        let mut last_match = 0;
        for cap in captures {
            let m = cap.get(0).unwrap();
            new.push_str(&out[last_match..m.start()]);
            rep.replace_append(&cap, &mut new);
            last_match = m.end();
        }
        new.push_str(&out[last_match..]);
        out = new;
    }

    Ok(Htmlblock {
        html: out,
        citeblocks,
        hash,
    })
}

#[cached(result = true, size = 8192, key = "u64", convert = "{hash}")]
async fn titleblock2htmlblock(
    titleblock: &[u8],
    hash: u64,
    cwd: &Path,
) -> Result<Option<Htmlblock>> {
    let mut cmd = Command::new("pandoc");
    cmd.current_dir(cwd)
        .arg("--from")
        .arg("markdown+emoji")
        .arg("--to")
        .arg("html5")
        .arg("--standalone")
        .arg("--katex");

    cmd.stdout(std::process::Stdio::piped());
    cmd.stdin(std::process::Stdio::piped());

    let mut child = cmd.spawn()?;
    let mut stdin = child.stdin.take().context("stdin take failed")?;

    stdin.write_all(titleblock).await?;

    // Send EOF to child
    drop(stdin);

    let out = child.wait_with_output().await?;
    let out = String::from_utf8(out.stdout)?;

    let fragment = scraper::Html::parse_fragment(&out);
    lazy_static! {
        static ref TITLE_SELECTOR: scraper::Selector =
            scraper::Selector::parse("header#title-block-header").unwrap();
    }

    if let Some(element) = fragment.select(&TITLE_SELECTOR).next() {
        Ok(Some(Htmlblock {
            html: element.html(),
            citeblocks: "".to_string(),
            hash,
        }))
    } else {
        Ok(None)
    }
}

const BIBKEYS: &[&str] = &[
    "bibliography",
    "csl",
    "link-citations",
    "nocite",
    "references",
];

fn mtime_from_file(file: &str, cwd: &Path) -> Result<u64> {
    let file = cwd.join(PathBuf::from(file));
    let mtime = std::fs::metadata(file)?
        .modified()?
        .duration_since(std::time::SystemTime::UNIX_EPOCH)?
        .as_secs();
    Ok(mtime)
}

async fn uniqueciteprocdict(
    split_md: &split_md::SplitMarkdown,
    htmlblocks: &Vec<Htmlblock>,
    cwd: &Path,
) -> Result<Option<Vec<u8>>> {
    let mut cloned_yaml_metadata = split_md.metadata.clone();
    cloned_yaml_metadata.retain(|k, _| BIBKEYS.contains(&k.as_str()));

    // No bib
    if cloned_yaml_metadata.is_empty() {
        return Ok(None);
    }

    // TODO: add capacity?
    let mut buf = Vec::new();

    // write metadata block
    writeln!(&mut buf, "---")?;

    // add mtimes of bib files etc.
    match cloned_yaml_metadata.get("bibliography") {
        Some(serde_yaml::Value::String(bibfile)) => {
            writeln!(
                &mut buf,
                "bibliography_mtime_: {}",
                mtime_from_file(bibfile, cwd)?
            )?;
        }
        Some(serde_yaml::Value::Sequence(bibs)) => {
            for (i, bibfile) in bibs.iter().enumerate() {
                if let serde_yaml::Value::String(bibfile) = bibfile {
                    writeln!(
                        &mut buf,
                        "bibliography_mtime_{}_: {}",
                        i,
                        mtime_from_file(bibfile, cwd)?
                    )?;
                }
            }
        }
        _ => {}
    }
    if let Some(serde_yaml::Value::String(cslfile)) = cloned_yaml_metadata.get("csl") {
        let mtime = mtime_from_file(cslfile, cwd)?;
        writeln!(&mut buf, "csl_mtime_: {}", mtime)?;
    }

    // write actual metadata
    write!(
        &mut buf,
        "{}---\n\n",
        &serde_yaml::to_string(&cloned_yaml_metadata)?
    )?;

    // write cite blocks
    for block in htmlblocks {
        write!(&mut buf, "{}\n\n", block.citeblocks)?;
    }

    Ok(Some(buf))
}

#[derive(Serialize)]
struct NewCiteprocMessage<'a> {
    html: &'a str,
    bibid: Option<u64>,
}

#[cached(result = true, size = 8192, key = "Option<u64>", convert = "{bibid}")]
async fn citeproc(
    bibid: Option<u64>,
    citeproc_input: Option<Vec<u8>>,
    cwd: &Path,
) -> Result<String> {
    let out = if let Some(citeproc_input) = citeproc_input {
        let mut cmd = Command::new("pandoc");
        cmd.current_dir(cwd)
            .arg("--from")
            .arg("markdown+emoji")
            .arg("--to")
            .arg("html5")
            .arg("--katex")
            .arg("--citeproc");

        cmd.stdout(std::process::Stdio::piped());
        cmd.stdin(std::process::Stdio::piped());

        let mut child = cmd.spawn()?;
        let mut stdin = child.stdin.take().context("stdin take failed")?;

        stdin.write_all(&citeproc_input).await?;

        // Send EOF to child
        drop(stdin);

        let out = child.wait_with_output().await?;
        String::from_utf8(out.stdout)?
    } else {
        "".to_string()
    };

    let message = NewCiteprocMessage { bibid, html: &out };

    let jsonmessage = serde_json::to_string(&message)?;
    Ok(jsonmessage)
}

#[derive(Serialize)]
struct NewContentMessage<'a> {
    filepath: &'a str,
    htmlblocks: &'a Vec<Htmlblock>,
    bibid: Option<u64>,
    #[serde(rename = "suppress-bibliography")]
    suppress_bibliography: bool,
    toc: bool,
    #[serde(rename = "toc-title")]
    toc_title: Option<&'a str>,
    #[serde(rename = "reference-section-title")]
    reference_section_title: &'a str,
}

const TITLEKEYS: &[&str] = &["title", "subtitle", "author", "date"];

// no cache, checks for bib differences
pub async fn md2htmlblocks<'a>(
    md: Bytes,
    fpath: &Path,
    cwd: &'a Path,
) -> Result<(String, impl futures::Future<Output = Result<String>> + 'a)> {
    let _start = std::time::Instant::now();

    let split_md = split_md::md2mdblocks(std::str::from_utf8(&md)?).await?;
    let htmlblocks = split_md.blocks.iter().map(|block| {
        let mut hasher = DefaultHasher::new();
        block.hash(&mut hasher);
        cwd.hash(&mut hasher);
        let hash = hasher.finish();

        mdblock2htmlblock(block, hash, cwd)
    });

    // Don't await htmlblocks right away so they can run in parallel with titleblock
    //
    // Note: The hot path during editing is (I think) most blocks = cached and one is
    // changed. Thus, all but one of the mdblock2htmlblock calls will be cached.
    // So `tokio::spawn`ing them here is probably not worth it and will just
    // generate overhead?
    let htmlblocks = futures::future::try_join_all(htmlblocks);

    let (mut htmlblocks, titleblock) =
        if split_md.titleblock.is_some() || split_md.metadata.contains_key("title") {
            // add title block, if any
            let mut cloned_yaml_metadata = split_md.metadata.clone();
            cloned_yaml_metadata.retain(|k, _| TITLEKEYS.contains(&k.as_str()));

            // TODO: add capacity?
            let mut buf = Vec::new();

            // write titleblock
            if let Some(ref titleblock) = split_md.titleblock {
                writeln!(buf, "{}", titleblock)?;
            }
            // write metadata block
            write!(
                &mut buf,
                "---\n{}---\n\n",
                &serde_yaml::to_string(&cloned_yaml_metadata)?
            )?;

            let titleblock = {
                let mut hasher = DefaultHasher::new();
                buf.hash(&mut hasher);
                cwd.hash(&mut hasher);
                let hash = hasher.finish();
                titleblock2htmlblock(&buf, hash, cwd)
            };

            futures::try_join!(htmlblocks, titleblock)?
        } else {
            (htmlblocks.await?, None)
        };

    if let Some(titleblock) = titleblock {
        htmlblocks.insert(0, titleblock);
    }

    let citejson = uniqueciteprocdict(&split_md, &htmlblocks, cwd).await?;
    let bibid = if let Some(ref citejson) = citejson {
        let mut hasher = DefaultHasher::new();
        citejson.hash(&mut hasher);
        cwd.hash(&mut hasher);
        Some(hasher.finish())
    } else {
        None
    };

    // Message to be sent to browser
    let message = NewContentMessage {
        filepath: fpath
            .to_str()
            .context("could not convert filepath to str")?, // TODO: relative to cwd?
        htmlblocks: &htmlblocks,
        bibid,
        suppress_bibliography: split_md.get_meta_flag("suppress-bibliography"),
        toc: split_md.get_meta_flag("toc"),
        toc_title: split_md.get_meta_str("toc-title"),
        reference_section_title: split_md
            .get_meta_str("reference-section-title")
            .unwrap_or(""),
    };

    debug!("md2htmlblocks (cmark) total = {:?}", _start.elapsed());

    let jsonmessage = serde_json::to_string(&message)?;
    Ok((jsonmessage, citeproc(bibid, citejson, cwd)))
}

#[cfg(test)]
mod tests {

    use crate::md_cmark::*;

    fn read_file(filename: &str) -> Result<(Bytes, PathBuf)> {
        let filepath = PathBuf::from(format!(
            "{}/resources/tests/{}",
            std::env::var("CARGO_MANIFEST_DIR")?,
            filename
        ));

        if filename.ends_with(".xz") {
            let mut f = std::io::BufReader::new(std::fs::File::open(&filepath)?);
            let mut decomp: Vec<u8> = Vec::new();
            lzma_rs::xz_decompress(&mut f, &mut decomp)?;

            Ok((
                decomp.into(),
                filepath.parent().context("no parent")?.to_path_buf(),
            ))
        } else {
            Ok((
                std::fs::read(&filepath)?.into(),
                filepath.parent().context("no parent")?.to_path_buf(),
            ))
        }
    }

    #[tokio::test]
    async fn md2htmlblocks_basic() -> Result<()> {
        let (md, cwd) = read_file("basic.md")?;
        let fpath = cwd.join("basic.md");
        let (json, _) =
            md2htmlblocks(md, fpath.as_path(), fpath.parent().context("no parent")?).await?;
        let json: serde_json::Value = serde_json::from_str(&json)?;
        assert_eq!(
            json.get("htmlblocks")
                .context("no htmlblocks")?
                .as_array()
                .context("no array")?[1]
                .as_array()
                .context("no array")?[1]
                .as_str()
                .context("no str")?
                .trim_end(),
            "<p>test</p>"
        );
        Ok(())
    }

    #[tokio::test]
    async fn md2htmlblocks_multiple_meta() -> Result<()> {
        let (md, cwd) = read_file("multiple-metablocks.md")?;
        let fpath = cwd.join("multiple-metablocks.md");
        let (json, _) =
            md2htmlblocks(md, fpath.as_path(), fpath.parent().context("no parent")?).await?;

        let json: serde_json::Value = serde_json::from_str(&json)?;
        assert_eq!(
            json.get("toc").context("no toc")?,
            &serde_json::Value::Bool(true)
        );
        assert_eq!(
            json.get("reference-section-title")
                .context("no ref section title")?,
            &serde_json::Value::String("My reference title".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn md2htmlblocks_twobibs_toc_relative_link() -> Result<()> {
        let (md, cwd) = read_file("two-bibs-toc-relative-link.md")?;
        let fpath = cwd.join("citations.md");
        let (json, citeproc_handle) =
            md2htmlblocks(md, fpath.as_path(), fpath.parent().context("no parent")?).await?;

        let json: serde_json::Value = serde_json::from_str(&json)?;
        let (expected, _) = read_file("two-bibs-toc-relative-link-linkblock.html")?;
        assert_eq!(
            json.get("htmlblocks")
                .context("no htmlblocks")?
                .as_array()
                .context("no array")?[4]
                .as_array()
                .context("no array")?[1]
                .as_str()
                .context("no str")?
                .trim_end(),
            std::str::from_utf8(&expected)?
                .replace("{cwd}", cwd.to_str().context("non-utf8 cwd")?)
                .trim_end()
        );

        let citeproc_out = citeproc_handle.await?;
        let citeproc_msg: serde_json::Value = serde_json::from_str(&citeproc_out)?;
        let (expected, _) = read_file("two-bibs-toc-relative-link-citeproc.html")?;
        assert_eq!(citeproc_msg["html"], std::str::from_utf8(&expected)?);

        Ok(())
    }

    #[tokio::test]
    async fn md2htmlblocks_bib() -> Result<()> {
        let (md, cwd) = read_file("citations.md")?;
        let fpath = cwd.join("citations.md");
        let (_, citeproc_handle) =
            md2htmlblocks(md, fpath.as_path(), fpath.parent().context("no parent")?).await?;
        let citeproc_out = citeproc_handle.await?;
        let citeproc_msg: serde_json::Value = serde_json::from_str(&citeproc_out)?;

        let (expected, _) = read_file("citations-citeproc.html")?;

        assert_eq!(citeproc_msg["html"], std::str::from_utf8(&expected)?);

        Ok(())
    }

    #[tokio::test]
    async fn md2htmlblocks_title() -> Result<()> {
        let (md, cwd) = read_file("title.md")?;
        let fpath = cwd.join("title.md");
        let (new_content, _) =
            md2htmlblocks(md, fpath.as_path(), fpath.parent().context("no parent")?).await?;
        let new_content: serde_json::Value = serde_json::from_str(&new_content)?;

        let (expected, _) = read_file("title-title.html")?;

        assert_eq!(
            new_content["htmlblocks"][0][1],
            std::str::from_utf8(&expected)?
        );
        Ok(())
    }
}
