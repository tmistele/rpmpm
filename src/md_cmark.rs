use anyhow::{Context, Result};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use tracing::debug;

use cached::proc_macro::cached;

use serde::Serialize;
use serde_tuple::Serialize_tuple;

use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use lazy_static::lazy_static;
use regex::{Regex as Regex, Replacer};

use bytes::Bytes;

struct ParsedBlock<'a> {
    footnote_references: Vec<pulldown_cmark::CowStr<'a>>,
    link_references: Vec<&'a str>,
    range: core::ops::Range<usize>,
}

type Metadata = std::collections::HashMap<String, serde_yaml::Value>;

#[derive(Clone)]
struct SplitMarkdown {
    metadata: Metadata,
    blocks: Vec<String>,
    titleblock: Option<String>,
}

impl<'a> SplitMarkdown {
    fn get_meta_flag(&self, name: &str) -> bool {
        if let Some(val) = self.metadata.get(name) {
            if let serde_yaml::Value::Bool(val) = val {
                *val
            } else {
                false
            }
        } else {
            false
        }
    }

    fn get_meta_str(&'a self, name: &str) -> Option<&'a str> {
        if let Some(val) = self.metadata.get(name) {
            if let serde_yaml::Value::String(val) = val {
                Some(val)
            } else {
                None
            }
        } else {
            None
        }
    }
}

fn try_parse_yaml_metadata_block(md: &str, start: usize) -> Option<(usize, Metadata)> {

    // See https://pandoc.org/MANUAL.html#extension-yaml_metadata_block
    
    // Not enough space to even contain a ---\n---
    if start + 7 > md.len() {
        return None
    }

    // It must be exactly three --- followed by newline
    if &md[start..start+4] != "---\n" {
        return None
    }

    // If initial --- is not at the beginning of the document, it must be preceded by a blank line
    if start > 1 && &md[start-2..start] != "\n\n" {
        return None
    }

    // The initial --- must not be followed by a blank line
    if &md[start+3..start+5] == "\n\n" {
        return None
    }
    
    // It must end with \n---\n or \n...\n or \n---EOF or \n...EOF
    // TODO: avoid scanning to end for nothing if it's \n...\n|EOF?
    let mut end = start+4;
    loop {
        let length = md[end..].find("\n---").or_else(|| md[end..].find("\n..."))?;
        end = end+length+4;

        if end == md.len() {
            // \n---EOF or \n...EOF
            break;
        } else if md[end..].starts_with("\n") {
            // \n---\n or \n...\n
            break;
        }
    }

    // It must be valid yaml
    let yaml = serde_yaml::from_str(&md[start+4..end-4]).ok()?;

    Some((end+1, yaml))
}

#[cached(result=true, size=10, key="u64", convert="{
    let mut hasher = DefaultHasher::new();
    md.hash(&mut hasher);
    hasher.finish()
}")]
async fn md2mdblocks(md: &str) -> Result<SplitMarkdown> {

    // Parse titleblock
    let (titleblock, md) = if md.starts_with('%') {
        let mut end = 0;
        let mut title = &md[1..];
        loop {
            let lineend = if let Some(lineend) = title.find('\n') {
                lineend
            } else {
                end += title.len()+1;
                return Ok(SplitMarkdown {
                    metadata: std::collections::HashMap::new(),
                    blocks: Vec::new(),
                    titleblock: Some(md[0..end].to_string()),
                })
            };
            title = &title[lineend+1..];
            end += lineend+1;
            if ! title.starts_with('%') && ! title.starts_with(' ') {
                break;
            }
        }
        (Some(md[0..end].to_string()), &md[end+1..])
    } else {
        (None, md)
    };

    // TODO: Can there be nested FootenoteDefinitions? I don't think so

    let mut options = pulldown_cmark::Options::empty();
    options.insert(pulldown_cmark::Options::ENABLE_FOOTNOTES);

    let parser = pulldown_cmark::Parser::new_ext(md, options);

    // TODO: Pandoc incompatibilty? Two footnote definitions without hard break
    //
    // Works in pandoc but not pulldown-cmark
    //
    // [^1]: asdf
    // [^4]: bsdf
    //
    // Works in both
    //
    // [^1]: asdf
    // 
    // [^4]: bsdf

    // Extract reference definitions, i.e. things like [foo]: http://www.example.com/
    // TODO: can I do w/o clone + to_owned() here?
    let link_reference_definitions: std::collections::HashMap<_, _> = parser.reference_definitions().iter().map(|(label, def)| (label.to_owned(), def.span.clone())).collect();

    let mut metadata: Option<Metadata> = None;

    let mut blocks = Vec::new(); // TODO: capacity? guess from last one?
    let mut current_block_footnote_references = Vec::new();
    let mut current_block_link_references = Vec::new();

    let mut footnote_definitions = std::collections::HashMap::new();

    let mut level = 0;
    let mut skip_until = 0;
    for (event, range) in parser.into_offset_iter() {

        // Maybe skip yaml metadata block we have parsed ourselves.
        if range.start < skip_until {
            continue;
        }

        match event {

            pulldown_cmark::Event::Start(pulldown_cmark::Tag::FootnoteDefinition(_)) => {
                level += 1;
            },
            pulldown_cmark::Event::End(pulldown_cmark::Tag::FootnoteDefinition(ref label)) => {
                footnote_definitions.insert(label.clone(), range);
                level -= 1;
            },

            pulldown_cmark::Event::Start(_) => {
                level += 1;
            },
            pulldown_cmark::Event::End(tag) => {
                level -= 1;

                if let pulldown_cmark::Tag::Link(pulldown_cmark::LinkType::Reference, _, _) |
                       pulldown_cmark::Tag::Link(pulldown_cmark::LinkType::Shortcut, _, _) = tag {
                    // TODO: ensure last is "]"
                    let mut end = range.end;
                    loop {
                        end = range.start + md[range.start..end].rfind("[").context("Missing [ in reference")?;
                        // label can contain ], it just has to be escaped as \]
                        if end == 0 || ! &md[end-1..].starts_with("\\") {
                            break;
                        }
                    }
                    let label = &md[end+1..range.end-1];
                    current_block_link_references.push(label);
                }

                if level == 0 {
                    blocks.push(ParsedBlock {
                        footnote_references: current_block_footnote_references,
                        link_references: current_block_link_references,
                        range: range,
                    });
                    current_block_footnote_references = Vec::new();
                    current_block_link_references = Vec::new();
                }
            },

            pulldown_cmark::Event::FootnoteReference(ref label) => {
                // TODO: Can this happen inside another footnote? I don't think so??
                current_block_footnote_references.push(label.clone());
            },

            pulldown_cmark::Event::Rule if level == 0 => {
                // A Rule may indicate a yaml metadata block
                if let Some((yaml_end, parsed_yaml)) = try_parse_yaml_metadata_block(md, range.start) {
                    if let Some(ref mut metadata) = metadata {
                        // A second/third/... metadata block. Merge it into the existing one
                        for (key, value) in parsed_yaml.into_iter() {
                            metadata.insert(key, value);
                        }
                    } else {
                        // First metadata block, use it directly
                        metadata = Some(parsed_yaml);
                    }
                    skip_until = yaml_end
                } else {
                    // A top-level Rule
                    blocks.push(ParsedBlock {
                        footnote_references: Vec::new(),
                        link_references: Vec::new(),
                        range: range,
                    });
                }
            },

            _ => if level == 0 {
                // A single-event top-level block, e.g. a Rule
                // TODO: can this happen actually? Rule is handled separately now...
                blocks.push(ParsedBlock {
                    footnote_references: Vec::new(),
                    link_references: Vec::new(),
                    range: range,
                });
            }
        }
    }

    // TODO: save source locations, at least at block-level? -- maybe we can use these to
    //       implement "jump to block" in editor?

    // TODO: pmpm right now = references to same footnote in different blocks = footnote appears
    //       twice. Here, we maybe can improve on this since we anyway parse the thing?

    // TODO: referencing footnotes in `title: ...` does not work (also doesn't work in normal pmpm)

    let blocks = blocks.iter().map(|block| {
        let length = (block.range.end - block.range.start) + 1
            + block.link_references
                .iter()
                .map(|label| link_reference_definitions.get(*label)
                    .map_or(0, |range| range.end - range.start + 1)).sum::<usize>()
            + block.footnote_references
                .iter()
                .map(|label| footnote_definitions.get(label)
                    .map_or(0, |range| range.end - range.start)).sum::<usize>();

        let mut buf = String::with_capacity(length);
        // block content
        write!(buf, "{}\n", &md[block.range.start..block.range.end])?;
        // add definitions
        for label in &block.link_references {
            // don't just unwrap in case of missing definition (common while typing!)
            if let Some(range) = link_reference_definitions.get(*label) {
                write!(buf, "{}\n", &md[range.start..range.end])?;
            }
        }
        // add footnotes
        for label in &block.footnote_references {
            // don't just unwrap in case of missing definition (common while typing!)
            if let Some(range) = footnote_definitions.get(label) {
                write!(buf, "{}", &md[range.start..range.end])?;
            }
        }

        Ok(buf)
    }).collect::<Result<Vec<_>>>()?;

    Ok(SplitMarkdown {
        metadata: metadata.unwrap_or_else(|| std::collections::HashMap::new()),
        blocks: blocks,
        titleblock: titleblock,
    })
}

#[derive(Serialize_tuple, Clone)]
struct Htmlblock {
    hash: u64,
    html: String,
    #[serde(skip_serializing)]
    citeblocks: String,
}

#[cached(result=true, size=8192, key="u64", convert="{hash}")]
async fn mdblock2htmlblock(md_block: &str, hash: u64, cwd: &Path) -> Result<Htmlblock> {

    let mut cmd = Command::new("pandoc");
    cmd
        .current_dir(cwd)
        .arg("--from").arg("markdown+emoji")
        .arg("--to").arg("html5")
        .arg("--katex");

    cmd.stdout(std::process::Stdio::piped());
    cmd.stdin(std::process::Stdio::piped());

    let mut child = cmd.spawn()?;
    let mut stdin = child.stdin.take().expect("stdin take failed");

    stdin.write_all(md_block.as_bytes()).await?;

    // Send EOF to child
    drop(stdin);

    let out = child.wait_with_output().await?;
    let mut out = String::from_utf8(out.stdout)?;

    let fragment = scraper::Html::parse_fragment(&out);
    lazy_static! {
        static ref CITE_SELECTOR: scraper::Selector = scraper::Selector::parse("span.citation").unwrap();
    }

    let citeblocks: String = fragment.select(&CITE_SELECTOR)
        .map(|element| element.text().next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n\n");

    // TODO: replace URL_REGEX thing? Now that we anyway parse the html?
    // Replace relative local URLs
    // Alternative is fancy_regex which supports negative backtracking.
    // But this here is faster for the common case where there are only few local links.
    lazy_static! {
        static ref URL_REGEX: Regex = Regex::new(r#"(href|src)=['"](.+?)['"]"#).unwrap();
        static ref URL_REGEX_EXCLUDE_PREFIX: Regex = Regex::new(r##"^/|https://|http://|\#"##).unwrap();
    }
    let captures: Vec<regex::Captures> = URL_REGEX.captures_iter(&out)
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
        citeblocks: citeblocks,
        hash: hash
    })
}

#[cached(result=true, size=8192, key="u64", convert="{hash}")]
async fn titleblock2htmlblock(titleblock: &Vec<u8>, hash: u64, cwd: &Path) -> Result<Option<Htmlblock>> {
    let mut cmd = Command::new("pandoc");
    cmd
        .current_dir(cwd)
        .arg("--from").arg("markdown+emoji")
        .arg("--to").arg("html5")
        .arg("--standalone")
        .arg("--katex");

    cmd.stdout(std::process::Stdio::piped());
    cmd.stdin(std::process::Stdio::piped());

    let mut child = cmd.spawn()?;
    let mut stdin = child.stdin.take().expect("stdin take failed");

    stdin.write_all(titleblock).await?;

    // Send EOF to child
    drop(stdin);

    let out = child.wait_with_output().await?;
    let out = String::from_utf8(out.stdout)?;

    let fragment = scraper::Html::parse_fragment(&out);
    lazy_static! {
        static ref TITLE_SELECTOR: scraper::Selector = scraper::Selector::parse("header#title-block-header").unwrap();
    }

    if let Some(element) = fragment.select(&TITLE_SELECTOR).next() {
        Ok(Some(Htmlblock {
            html: element.html(),
            citeblocks: "".to_string(),
            hash: hash
        }))
    } else {
        Ok(None)

    }
}


const BIBKEYS: &'static [&'static str] = &["bibliography", "csl", "link-citations", "nocite", "references"];

fn mtime_from_file(file: &str, cwd: &Path) -> Result<u64> {
    let file = cwd.join(PathBuf::from(file));
    let mtime = std::fs::metadata(file)?.modified()?.duration_since(std::time::SystemTime::UNIX_EPOCH)?.as_secs();
    return Ok(mtime);
}

async fn uniqueciteprocdict(split_md: &SplitMarkdown, htmlblocks: &Vec<Htmlblock>, cwd: &Path) -> Result<Option<Vec<u8>>> {

    let mut cloned_yaml_metadata = split_md.metadata.clone();
    cloned_yaml_metadata.retain(|k, _| BIBKEYS.contains(&k.as_str()));

    // No bib
    if cloned_yaml_metadata.len() <= 0 {
        return Ok(None);
    }

    // TODO: add capacity?
    let mut buf = Vec::new();

    // write metadata block
    write!(&mut buf, "---\n")?;

    // add mtimes of bib files etc.
    match cloned_yaml_metadata.get("bibliography") {
        Some(serde_yaml::Value::String(bibfile)) => {
            write!(&mut buf, "bibliography_mtime_: {}\n", mtime_from_file(&bibfile, cwd)?)?;
        },
        Some(serde_yaml::Value::Sequence(bibs)) => {
            for (i, bibfile) in bibs.iter().enumerate() {
                if let serde_yaml::Value::String(bibfile) = bibfile {
                    write!(&mut buf, "bibliography_mtime_{}_: {}\n", i, mtime_from_file(&bibfile, cwd)?)?;
                }
            }
        },
        _ => {},
    }
    if let Some(serde_yaml::Value::String(cslfile)) = cloned_yaml_metadata.get("csl") {
        let mtime = mtime_from_file(&cslfile, cwd)?;
        write!(&mut buf, "csl_mtime_: {}\n", mtime)?;
    }

    // write actual metadata
    write!(&mut buf, "{}---\n\n", &serde_yaml::to_string(&cloned_yaml_metadata)?)?;

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

#[cached(result=true, size=8192, key="Option<u64>", convert="{bibid}")]
async fn citeproc(bibid: Option<u64>, citeproc_input: Option<Vec<u8>>, cwd: &Path) -> Result<String> {

    let out = if let Some(citeproc_input) = citeproc_input {
        let mut cmd = Command::new("pandoc");
        cmd.current_dir(cwd)
            .arg("--from").arg("markdown+emoji")
            .arg("--to").arg("html5")
            .arg("--katex")
            .arg("--citeproc");

        cmd.stdout(std::process::Stdio::piped());
        cmd.stdin(std::process::Stdio::piped());

        let mut child = cmd.spawn()?;
        let mut stdin = child.stdin.take().expect("stdin take failed");

        stdin.write_all(&citeproc_input).await?;

        // Send EOF to child
        drop(stdin);

        let out = child.wait_with_output().await?;
        String::from_utf8(out.stdout)?
    } else {
        "".to_string()
    };

    let message = NewCiteprocMessage {
        bibid: bibid,
        html: &out,
    };

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

const TITLEKEYS: &'static [&'static str] = &["title", "subtitle", "author", "date"];

// no cache, checks for bib differences
pub async fn md2htmlblocks<'a>(md: Bytes, fpath: &Path, cwd: &'a Path) -> Result<(String, impl futures::Future<Output = Result<String> > + 'a)> {

    let _start = std::time::Instant::now();

    let split_md = md2mdblocks(std::str::from_utf8(&md)?).await?;
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

    let (mut htmlblocks, titleblock) = if split_md.titleblock.is_some() || split_md.metadata.contains_key("title") {

        // add title block, if any
        let mut cloned_yaml_metadata = split_md.metadata.clone();
        cloned_yaml_metadata.retain(|k, _| TITLEKEYS.contains(&k.as_str()));

        // TODO: add capacity?
        let mut buf = Vec::new();

        // write titleblock
        if let Some(ref titleblock) = split_md.titleblock {
            write!(buf, "{}\n", titleblock)?;
        }
        // write metadata block
        write!(&mut buf, "---\n{}---\n\n", &serde_yaml::to_string(&cloned_yaml_metadata)?)?;

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
        filepath: fpath.to_str().context("could not convert filepath to str")?, // TODO: relative to cwd?
        htmlblocks: &htmlblocks,
        bibid: bibid,
        suppress_bibliography: split_md.get_meta_flag("suppress-bibliography"),
        toc: split_md.get_meta_flag("toc"),
        toc_title: split_md.get_meta_str("toc-title"),
        reference_section_title: split_md.get_meta_str("reference-section-title").unwrap_or(""),
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
            std::env::var("CARGO_MANIFEST_DIR")?, filename));

        if filename.ends_with(".xz") {
            let mut f = std::io::BufReader::new(std::fs::File::open(&filepath)?);
            let mut decomp: Vec<u8> = Vec::new();
            lzma_rs::xz_decompress(&mut f, &mut decomp)?;

            Ok((decomp.into(), filepath.parent().context("no parent")?.to_path_buf()))
        } else {
            Ok((std::fs::read(&filepath)?.into(), filepath.parent().context("no parent")?.to_path_buf()))
        }
    }

    #[tokio::test]
    async fn split_md_metadata_block_at_start_with_one_newline() -> Result<()> {
        let md = "\n---\ntoc: true\n---\n";
        assert_eq!(md2mdblocks(md).await?.metadata.get("toc"), Some(&serde_yaml::Value::Bool(true)));

        let md = "\n\n---\ntoc: true\n---\n";
        assert_eq!(md2mdblocks(md).await?.metadata.get("toc"), Some(&serde_yaml::Value::Bool(true)));

        let md = "\nasdf\n---\ntoc: true\n---\n";
        assert_eq!(md2mdblocks(md).await?.metadata.get("toc"), None);

        let md = "asdf\n---\ntoc: true\n---\n";
        assert_eq!(md2mdblocks(md).await?.metadata.get("toc"), None);

        let md = "\n---\ntoc: true\n---";
        assert_eq!(md2mdblocks(md).await?.metadata.get("toc"), Some(&serde_yaml::Value::Bool(true)));

        let md = "\n---\ntoc: true\n---\n\nasdf";
        let split_md = md2mdblocks(md).await?;
        assert_eq!(split_md.metadata.get("toc"), Some(&serde_yaml::Value::Bool(true)));
        assert_eq!(split_md.blocks.len(), 1);
        assert_eq!(split_md.blocks[0], "asdf\n");

        Ok(())
    }

    #[tokio::test]
    async fn split_md_titleblock() -> Result<()> {
        let md = "%asdf1\n asdf2\n asdf3\nasdf4";
        let split_md = md2mdblocks(md).await?;
        assert_eq!(split_md.titleblock, Some("%asdf1\n asdf2\n asdf3".to_string()));
        assert_eq!(split_md.blocks.len(), 1);
        assert_eq!(split_md.blocks[0], "asdf4\n");
        assert_eq!(split_md.metadata.len(), 0);

        let md = "%asdf1\n asdf2";
        assert_eq!(md2mdblocks(md).await?.titleblock, Some("%asdf1\n asdf2".to_string()));

        let md = " %asdf1\n asdf2";
        assert_eq!(md2mdblocks(md).await?.titleblock, None);

        let md = "asdf\n%asdf1\n asdf2";
        assert_eq!(md2mdblocks(md).await?.titleblock, None);

        Ok(())
    }


    #[tokio::test]
    async fn split_md_footnote_links() -> Result<()> {
        let (md, _) = read_file("footnotes-links.md")?;
        let md = std::str::from_utf8(&md)?;
        let split = md2mdblocks(md).await?;
        assert_eq!(split.blocks.len(), 5);
        assert_eq!(split.blocks[2].trim_end(), "---");
        assert_eq!(
            split.blocks[3].trim_end(),
            "test [^1] [another][link_with_title_andbracket\\[_name] goes here [^4]\n\n\
                [link_with_title_andbracket\\[_name]: https://github.com/two \"title with \\\"quotes\\\" asdf\"\n\
                [^1]: content of footnote\n\
                \n\
                [^4]: footnote with $x\\\\y=1$ math and stuff $\\frac12 = x$");
        assert_eq!(split.metadata.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn split_md_multiple_metablocks() -> Result<()> {
        let (md, _) = read_file("multiple-metablocks.md")?;
        let md = std::str::from_utf8(&md)?;
        let split = md2mdblocks(md).await?;
        assert_eq!(split.blocks.len(), 6);
        assert_eq!(split.metadata.len(), 4);
        assert_eq!(split.metadata.get("title"), Some(&serde_yaml::Value::String("my title".to_string())));
        Ok(())
    }

    #[tokio::test]
    async fn md2htmlblocks_basic() -> Result<()> {
        let (md, cwd) = read_file("basic.md")?;
        let fpath = cwd.join("basic.md");
        let (json, _) = md2htmlblocks(md, fpath.as_path(), fpath.parent().context("no parent")?).await?;
        let json: serde_json::Value = serde_json::from_str(&json)?;
        assert_eq!(
            json.get("htmlblocks").context("no htmlblocks")?.as_array().context("no array")?[1].as_array().context("no array")?[1].as_str().context("no str")?.trim_end(),
            "<p>test</p>"
        );
        Ok(())
    }

    #[tokio::test]
    async fn md2htmlblocks_multiple_meta() -> Result<()> {
        let (md, cwd) = read_file("multiple-metablocks.md")?;
        let fpath = cwd.join("multiple-metablocks.md");
        let (json, _) = md2htmlblocks(md, fpath.as_path(), fpath.parent().context("no parent")?).await?;

        let json: serde_json::Value = serde_json::from_str(&json)?;
        assert_eq!(json.get("toc").context("no toc")?, &serde_json::Value::Bool(true));
        assert_eq!(json.get("reference-section-title").context("no ref section title")?, &serde_json::Value::String("My reference title".to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn md2htmlblocks_twobibs_toc_relative_link() -> Result<()> {
        let (md, cwd) = read_file("two-bibs-toc-relative-link.md")?;
        let fpath = cwd.join("citations.md");
        let (json, citeproc_handle) = md2htmlblocks(md, fpath.as_path(), fpath.parent().context("no parent")?).await?;

        let json: serde_json::Value = serde_json::from_str(&json)?;
        let (expected, _) = read_file("two-bibs-toc-relative-link-linkblock.html")?;
        assert_eq!(
            json.get("htmlblocks").context("no htmlblocks")?.as_array().context("no array")?[4].as_array().context("no array")?[1].as_str().context("no str")?.trim_end(),
            std::str::from_utf8(&expected)?.replace("{cwd}", cwd.to_str().context("non-utf8 cwd")?).trim_end()
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
        let (_, citeproc_handle) = md2htmlblocks(md, fpath.as_path(), fpath.parent().context("no parent")?).await?;
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
        let (new_content, _) = md2htmlblocks(md, fpath.as_path(), fpath.parent().context("no parent")?).await?;
        let new_content: serde_json::Value = serde_json::from_str(&new_content)?;

        let (expected, _) = read_file("title-title.html")?;

        assert_eq!(new_content["htmlblocks"][0][1], std::str::from_utf8(&expected)?);
        Ok(())
    }
}
