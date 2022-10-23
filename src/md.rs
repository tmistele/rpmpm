use anyhow::{Context, Result, anyhow};

use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use cached::proc_macro::cached;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_tuple::Serialize_tuple;

use lazy_static::lazy_static;
use regex::{Regex as Regex, Replacer};

use bytes::Bytes;

type RawPandocBlock = serde_json::value::RawValue;
type RawPandocApiVersion = serde_json::value::RawValue;

#[derive(Serialize, Deserialize, Debug)]
struct PandocDoc<'a> {
    #[serde(rename = "pandoc-api-version")]
    pandoc_api_version: &'a RawPandocApiVersion,
    meta: serde_json::Map<String, serde_json::Value>,
    #[serde(borrow)]
    blocks: Vec<&'a RawPandocBlock>,
}

#[derive(Serialize, Debug)]
struct PandocDocNonRawBlocks<'a> {
    #[serde(rename = "pandoc-api-version")]
    pandoc_api_version: &'a RawPandocApiVersion,
    meta: serde_json::Map<String, serde_json::Value>,
    blocks: Vec<&'a serde_json::Value>,
}

impl<'a> PandocDoc<'a> {
    fn get_meta_flag(&self, name: &str) -> bool {
        if let Some(val) = self.meta.get(name) {
            if let serde_json::Value::Bool(flag) = val["c"] {
                flag
            } else {
                false
            }
        } else {
            false
        }
    }

    fn get_meta_str(&'a self, name: &str) -> Option<&'a str> {
        if let Some(val) = self.meta.get(name) {
            if let serde_json::Value::String(string) = &val["c"][0]["c"] {
                Some(string)
            } else {
                None
            }
        } else {
            None
        }
    }
}


#[cached(result=true, size=10, key="u64", convert="{
    let mut hasher = DefaultHasher::new();
    md.hash(&mut hasher);
    cwd.hash(&mut hasher);
    hasher.finish()
}")]
async fn md2json(md: &Bytes, cwd: &Path) -> Result<String> {
    let mut cmd = Command::new("pandoc");
    cmd
        .current_dir(cwd)
        .arg("--from").arg("markdown+emoji")
        .arg("--to").arg("json")
        .arg("--katex");

    cmd.stdout(std::process::Stdio::piped());
    cmd.stdin(std::process::Stdio::piped());

    let mut child = cmd.spawn()?;
    let mut stdin = child.stdin.take().expect("stdin take failed");

    stdin.write_all(&md).await?;

    // Send EOF to child
    drop(stdin);

    let out = child.wait_with_output().await?;
    Ok(String::from_utf8(out.stdout)?)
}

fn get_citeblocks(block: &serde_json::Value, list: &mut Vec<serde_json::Value>) -> () {
    match block {
        serde_json::Value::Object(map) => {
            if let Some(ty) = map.get("t") {
                if ty == "Cite" {
                    list.push(serde_json::json!({"t": "Para", "c": [block.clone()]}));
                    return;
                }
            }

            for block in map.values() {
                get_citeblocks(block, list);
            }

        },
        serde_json::Value::Array(vec) => {
            for block in vec {
                get_citeblocks(block, list);
            }
        },
        _ => {}
    };
}

#[derive(Serialize_tuple, Clone)]
struct Htmlblock {
    hash: u64,
    html: String,
    #[serde(skip_serializing)]
    citeblocks: Vec<serde_json::Value>,
}

#[cached(result=true, size=8192, key="u64", convert="{hash}")]
async fn json2htmlblock(pandoc_api_version: &RawPandocApiVersion, block: &RawPandocBlock, hash: u64, cwd: &Path) -> Result<Htmlblock> {
    let mut cmd = Command::new("pandoc");
    cmd
        .current_dir(cwd)
        .arg("--from").arg("json")
        .arg("--to").arg("html5")
        .arg("--katex");

    cmd.stdout(std::process::Stdio::piped());
    cmd.stdin(std::process::Stdio::piped());

    let mut child = cmd.spawn()?;
    let mut stdin = child.stdin.take().expect("stdin take failed");

    let doc = PandocDoc {
        pandoc_api_version: pandoc_api_version,
        blocks: vec![block],
        meta: serde_json::Map::new(),
    };
    let json = serde_json::to_vec(&doc)?;

    stdin.write_all(&json).await?;

    // Send EOF to child
    drop(stdin);

    let out = child.wait_with_output().await?;
    let mut out = String::from_utf8(out.stdout)?;

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

    // find citecproc elements
    let parsed_block: serde_json::Value = serde_json::from_str(block.get())?;

    let mut citeblocks: Vec<serde_json::Value> = Vec::new();
    get_citeblocks(&parsed_block, &mut citeblocks);

    Ok(Htmlblock {
        html: out,
        citeblocks: citeblocks,
        hash: hash
    })
}

#[cached(result=true, size=8192, key="u64", convert="{hash}")]
async fn json2titleblock(json: &Vec<u8>, hash: u64, cwd: &Path) -> Result<Option<Htmlblock>> {
    println!("doing json2titleblock!");

    let mut cmd = Command::new("pandoc");
    cmd
        .current_dir(cwd)
        .arg("--from").arg("json")
        .arg("--to").arg("html5")
        .arg("--standalone")
        .arg("--katex");

    cmd.stdout(std::process::Stdio::piped());
    cmd.stdin(std::process::Stdio::piped());

    let mut child = cmd.spawn()?;
    let mut stdin = child.stdin.take().expect("stdin take failed");

    stdin.write_all(json).await?;

    // Send EOF to child
    drop(stdin);

    let out = child.wait_with_output().await?;
    let out = String::from_utf8(out.stdout)?;

    let start = out.find("<header id=\"title-block-header\">");
    if let Some(start) = start {
        let end = out[start..].find("</header>").context("title block end not found")?;
        let out = out[start..(start+end+9)].to_string();

        Ok(Some(Htmlblock {
            html: out,
            citeblocks: vec![],
            hash: hash
        }))
    } else {
        Ok(None)

    }
}

const BIBKEYS: &'static [&'static str] = &["bibliography", "csl", "link-citations", "nocite", "references"];

fn mtime_from_meta_bibliography(bibliography: &serde_json::Map<String, serde_json::Value>, cwd: &Path) -> Result<u64> {
    if let serde_json::Value::String(bibfile) = &bibliography["c"][0]["c"] {
        let bibfile = cwd.join(PathBuf::from(bibfile));
        let mtime = fs::metadata(bibfile)?.modified()?.duration_since(std::time::SystemTime::UNIX_EPOCH)?.as_secs();
        return Ok(mtime);
    } else {
        Err(anyhow!("Unexpected json structure in bibliography"))
    }
}

async fn uniqueciteprocdict(doc: &PandocDoc<'_>, htmlblocks: &Vec<Htmlblock>, cwd: &Path) -> Result<Option<Vec<u8>>> {
    // collect all cite blocks
    let citeblocks = htmlblocks.iter().map(|b| &b.citeblocks).flatten().collect::<Vec<&serde_json::Value>>();

    let mut doc = PandocDocNonRawBlocks {
        pandoc_api_version: doc.pandoc_api_version,
        blocks: citeblocks,
        meta: {
            let mut cloned = doc.meta.clone();
            cloned.retain(|k, _| BIBKEYS.contains(&k.as_str()));
            cloned
        }
    };

    // No bib
    if doc.meta.len() <= 0 {
        return Ok(None);
    }
    
    // .bib mtimes
    if let Some(serde_json::Value::Object(bibliography)) = doc.meta.get_mut("bibliography") {

        if bibliography["t"] == "MetaInlines" {
            let mtime = mtime_from_meta_bibliography(bibliography, cwd)?;
            bibliography.insert("bibliography_mtimes_".to_string(), serde_json::json!(mtime));
        } else if let serde_json::Value::Array(bibs) = &bibliography["c"] {
            let mtimes = bibs.iter().map(|bibliography| {
                    let bibliography = if let serde_json::Value::Object(x) = bibliography { x } else { return Err(anyhow!("bibliography not an Object")) };
                    let mtime = mtime_from_meta_bibliography(bibliography, cwd)?;
                    Ok::<serde_json::Value, anyhow::Error>(serde_json::json!(mtime))
                })
                .collect::<Result<Vec<serde_json::Value>>>()?;
            bibliography.insert("bibliography_mtimes_".to_string(), serde_json::Value::Array(mtimes));
        }
    }

    // .csl mtime
    if let Some(serde_json::Value::Object(csl)) = doc.meta.get_mut("csl") {
        let mtime = mtime_from_meta_bibliography(&csl, cwd)?;
            csl.insert("csl_mtime_".to_string(), serde_json::json!(mtime));
    }

    let json = serde_json::to_vec(&doc)?;

    Ok(Some(json))
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
            .arg("--from").arg("json")
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

    println!("citeproc reuslt length = {}", out.len());

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

    let doc = md2json(&md, cwd).await?;
    // This json parse is not cached. But the dominant thing should be the blocks which are just
    // borrowed `RawValue`s so this should still be pretty fast?
    let doc: PandocDoc = serde_json::from_str(&doc)?;

    let htmlblocks = doc.blocks.iter().map(|block| {
        let mut hasher = DefaultHasher::new();
        block.get().hash(&mut hasher);
        cwd.hash(&mut hasher);
        let hash = hasher.finish();

        json2htmlblock(doc.pandoc_api_version, block, hash, cwd)
    });

    // Don't await htmlblocks right away so they can run in parallel with titleblock
    //
    // Note: The hot path during editing is (I think) most blocks = cached and one is
    // changed. Thus, all but one of the json2htmlblock calls will be cached.
    // So `tokio::spawn`ing them here is probably not worth it and will just
    // generate overhead?
    let htmlblocks = futures::future::try_join_all(htmlblocks);

    let (mut htmlblocks, titleblock) = if doc.meta.contains_key("title") {

        // add title block, if any
        let titledoc = PandocDoc {
            pandoc_api_version: doc.pandoc_api_version,
            blocks: vec![],
            meta: {
                let mut cloned = doc.meta.clone();
                cloned.retain(|k, _| TITLEKEYS.contains(&k.as_str()));
                cloned
            }
        };
        let titlejson = serde_json::to_vec(&titledoc)?;
        let titleblock = {
            let mut hasher = DefaultHasher::new();
            titlejson.hash(&mut hasher);
            cwd.hash(&mut hasher);
            let hash = hasher.finish();
            json2titleblock(&titlejson, hash, cwd)
        };

        futures::try_join!(htmlblocks, titleblock)?
    } else {
        (htmlblocks.await?, None)
    };

    if let Some(titleblock) = titleblock {
        htmlblocks.insert(0, titleblock);
    }

    let citejson = uniqueciteprocdict(&doc, &htmlblocks, cwd).await?;
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
        suppress_bibliography: doc.get_meta_flag("suppress-bibliography"),
        toc: doc.get_meta_flag("toc"),
        toc_title: doc.get_meta_str("toc-title"),
        reference_section_title: doc.get_meta_str("reference-section-title").unwrap_or(""),
    };

    println!("md2htmlblocks total = {:?}", _start.elapsed());

    let jsonmessage = serde_json::to_string(&message)?;
    Ok((jsonmessage, citeproc(bibid, citejson, cwd)))
}

#[cfg(test)]
mod tests {
    
    use crate::md::*;

    fn read_file(filename: &str) -> (Bytes, PathBuf) {
        let filepath = PathBuf::from(format!(
            "{}/resources/tests/{}",
            std::env::var("CARGO_MANIFEST_DIR").unwrap(), filename));

        if filename.ends_with(".xz") {
            let mut f = std::io::BufReader::new(std::fs::File::open(&filepath).unwrap());
            let mut decomp: Vec<u8> = Vec::new();
            lzma_rs::xz_decompress(&mut f, &mut decomp).unwrap();

            (decomp.into(), filepath.parent().unwrap().to_path_buf())
        } else {
            (fs::read(&filepath).unwrap().into(), filepath.parent().unwrap().to_path_buf())
        }

    }

    fn roundtripped_json(filename: &str) -> String {
        let (json, _) = read_file(filename);
        let json = std::str::from_utf8(&json).unwrap();
        let json: serde_json::Value = serde_json::from_str(json).unwrap();
        serde_json::to_string(&json).unwrap()
    }

    fn doc_to_json_roundtrip(doc: &PandocDoc) -> String {
        let json = serde_json::to_string(&doc).unwrap();
        let json: serde_json::Value = serde_json::from_str(&json).unwrap();
        serde_json::to_string(&json).unwrap()
    }
    
    #[tokio::test]
    async fn md2json_basic() {
        let (md, cwd) = read_file("basic.md");
        let json = roundtripped_json("basic.json");

        let doc = md2json(&md, cwd.as_path()).await.unwrap();
        let doc: PandocDoc = serde_json::from_str(&doc).unwrap();

        assert_eq!(doc_to_json_roundtrip(&doc), json);
    }

    #[tokio::test]
    async fn md2json_long() {
        // Longer than 64kb which may be larger than pipe
        // That's why we need `write_all` and not just `write`
        let (md, cwd) = read_file("long.md.xz");
        let json = roundtripped_json("long.json.xz");

        let doc = md2json(&md, cwd.as_path()).await.unwrap();
        let doc: PandocDoc = serde_json::from_str(&doc).unwrap();

        assert_eq!(doc_to_json_roundtrip(&doc), json);
    }

    #[tokio::test]
    async fn md2htmlblocks_bib() {
        let (md, cwd) = read_file("citations.md");
        let fpath = cwd.join("citations.md");
        let (_, citeproc_handle) = md2htmlblocks(md, fpath.as_path(), fpath.parent().unwrap()).await.unwrap();
        let citeproc_out = citeproc_handle.await.unwrap();
        let citeproc_msg: serde_json::Value = serde_json::from_str(&citeproc_out).unwrap();

        let (expected, _) = read_file("citations-citeproc.html");

        assert_eq!(citeproc_msg["html"], std::str::from_utf8(&expected).unwrap());
    }

    #[tokio::test]
    async fn md2htmlblocks_twobibs_toc_relative_link() {
        let (md, cwd) = read_file("two-bibs-toc-relative-link.md");
        let fpath = cwd.join("citations.md");
        let (_, citeproc_handle) = md2htmlblocks(md, fpath.as_path(), fpath.parent().unwrap()).await.unwrap();
        let citeproc_out = citeproc_handle.await.unwrap();
        let citeproc_msg: serde_json::Value = serde_json::from_str(&citeproc_out).unwrap();

        let (expected, _) = read_file("two-bibs-toc-relative-link-citeproc.html");

        assert_eq!(citeproc_msg["html"], std::str::from_utf8(&expected).unwrap());
    }

    #[tokio::test]
    async fn md2htmlblocks_title() {
        let (md, cwd) = read_file("title.md");
        let fpath = cwd.join("title.md");
        let (new_content, _) = md2htmlblocks(md, fpath.as_path(), fpath.parent().unwrap()).await.unwrap();
        let new_content: serde_json::Value = serde_json::from_str(&new_content).unwrap();

        let (expected, _) = read_file("title-title.html");

        assert_eq!(new_content["htmlblocks"][0][1], std::str::from_utf8(&expected).unwrap());
    }
}
