use anyhow::{Context, Result};

use std::collections::hash_map::DefaultHasher;
use std::fmt::Write as _;
use std::hash::{Hash, Hasher};

use cached::proc_macro::cached;

struct ParsedBlock<'a> {
    footnote_references: Vec<pulldown_cmark::CowStr<'a>>,
    link_references: Vec<&'a str>,
    range: core::ops::Range<usize>,
}

type Metadata = std::collections::HashMap<String, serde_yaml::Value>;

#[derive(Clone)]
pub struct SplitMarkdown {
    pub metadata: Metadata,
    pub blocks: Vec<String>,
    pub titleblock: Option<String>,
}

impl<'a> SplitMarkdown {
    pub fn get_meta_flag(&self, name: &str) -> bool {
        self.metadata
            .get(name)
            .map(|val| val.as_bool().unwrap_or(false))
            .unwrap_or(false)
    }

    pub fn get_meta_str(&'a self, name: &str) -> Option<&'a str> {
        self.metadata.get(name)?.as_str()
    }
}

fn try_parse_yaml_metadata_block(md: &str, start: usize) -> Option<(usize, Metadata)> {
    // See https://pandoc.org/MANUAL.html#extension-yaml_metadata_block

    // Not enough space to even contain a ---\n---
    if start + 7 > md.len() {
        return None;
    }

    // It must be exactly three --- followed by newline
    if &md[start..start + 4] != "---\n" {
        return None;
    }

    // If initial --- is not at the beginning of the document, it must be preceded by a blank line
    if start > 1 && &md[start - 2..start] != "\n\n" {
        return None;
    }

    // The initial --- must not be followed by a blank line
    if &md[start + 3..start + 5] == "\n\n" {
        return None;
    }

    // It must end with \n---\n or \n...\n or \n---EOF or \n...EOF
    // TODO: avoid scanning to end for nothing if it's \n...\n|EOF?
    let mut end = start + 4;
    loop {
        let length = md[end..]
            .find("\n---")
            .or_else(|| md[end..].find("\n..."))?;
        end = end + length + 4;

        // \n---EOF or \n...EOF
        // or
        // \n---\n or \n...\n
        if end == md.len() || md[end..].starts_with('\n') {
            break;
        }
    }

    // It must be valid yaml
    let yaml = serde_yaml::from_str(&md[start + 4..end - 4]).ok()?;

    Some((end + 1, yaml))
}

fn scan_multiline_footnote(md: &str, end: usize) -> usize {
    // Find first non-whitespace non-newline
    let mut indent = 0;
    let mut pos = end;
    for (i, c) in md[end..].bytes().enumerate() {
        if c == b'\n' {
            indent = 0;
        } else if c == b' ' {
            indent += 1;
        } else {
            pos += i;
            break;
        }
    }

    // If not indented, footnote doesn't continue
    // Trying it out: 4 spaces are needed in pandoc?
    if indent < 4 {
        return end;
    }

    // This indented line belongs to the footnote definition
    pos += if let Some(line) = md[pos..].find('\n') {
        line
    } else {
        return md.len();
    };

    // Scan until first unindented/underindented line after a purely whitespace line
    let mut prev_line_blank = false;
    loop {
        // At position `pos`, we have a \n. Ignore it.
        if md.len() > pos {
            pos += 1;
        } else {
            break;
        }

        // Scan the line, checking if it is blank or unindented/underindented
        let mut nl = None;
        let mut blank = true;
        let mut indent = 0;
        for (i, c) in md[pos..].bytes().enumerate() {
            if c == b'\n' {
                nl = Some(i);
                break;
            } else if c == b' ' {
                if blank {
                    indent += 1;
                }
            } else {
                blank = false;
            }
        }

        if blank {
            // Blank lines are ok
            prev_line_blank = true;
        } else {
            // Non-blank lines must be correctly indented, unless the previous line was not blank
            if indent < 4 && prev_line_blank {
                // The current line does no longer belong to the footnote
                break;
            }
            prev_line_blank = false;
        }

        if let Some(nl) = nl {
            // Go to next \n
            pos += nl;
        } else {
            // We reached EOF (didn't find another \n)
            pos += md[pos..].len();
            break;
        }
    }

    pos
}

struct Splitter<'a> {
    md: &'a str,
    offset_iter: pulldown_cmark::OffsetIter<'a, 'a>,
    titleblock: Option<String>,
    link_reference_definitions: std::collections::HashMap<String, std::ops::Range<usize>>,
    metadata: Option<Metadata>,
    blocks: Vec<ParsedBlock<'a>>,
    current_block_footnote_references: Vec<pulldown_cmark::CowStr<'a>>,
    current_block_link_references: Vec<&'a str>,
    level: i32,
    footnote_definitions:
        std::collections::HashMap<pulldown_cmark::CowStr<'a>, std::ops::Range<usize>>,
    in_footnote_definition: bool,
    current_footnote_definition_start: usize,
    current_footnote_definition_label: pulldown_cmark::CowStr<'a>,
}

impl<'a> Splitter<'a> {
    fn new(md: &'a str) -> Self {
        // Parse titleblock
        let (titleblock, md) = if let Some(mut title) = md.strip_prefix('%') {
            let mut end = 0;
            loop {
                let lineend = if let Some(lineend) = title.find('\n') {
                    lineend
                } else {
                    end += title.len() + 1;
                    break (Some(md[0..end].to_string()), "");
                };
                title = &title[lineend + 1..];
                end += lineend + 1;
                if !title.starts_with('%') && !title.starts_with(' ') {
                    break (Some(md[0..end].to_string()), &md[end + 1..]);
                }
            }
        } else {
            (None, md)
        };

        let mut options = pulldown_cmark::Options::empty();
        options.insert(pulldown_cmark::Options::ENABLE_FOOTNOTES);

        let parser = pulldown_cmark::Parser::new_ext(md, options);

        // Extract reference definitions, i.e. things like [foo]: http://www.example.com/
        // TODO: can I do w/o clone + to_owned() here?
        let link_reference_definitions: std::collections::HashMap<_, _> = parser
            .reference_definitions()
            .iter()
            .map(|(label, def)| (label.to_owned(), def.span.clone()))
            .collect();

        Splitter {
            md,
            offset_iter: parser.into_offset_iter(),
            titleblock,
            link_reference_definitions,
            metadata: None,
            blocks: Vec::new(), // TODO: capacity? guess from last one?
            current_block_footnote_references: Vec::new(),
            current_block_link_references: Vec::new(),
            level: 0,
            footnote_definitions: std::collections::HashMap::new(),
            in_footnote_definition: false,
            current_footnote_definition_start: 0,
            current_footnote_definition_label: pulldown_cmark::CowStr::Borrowed(""),
        }
    }

    fn split(&mut self) -> Result<()> {
        let mut skip_until = 0;
        while let Some((event, range)) = self.offset_iter.next() {
            // Skip e.g. yaml metadata block we parsed ourselves
            if range.start < skip_until {
                continue;
            }
            skip_until = self.step(self.md, event, range)?;
        }
        Ok(())
    }

    fn step(
        &mut self,
        md: &'a str,
        event: pulldown_cmark::Event<'a>,
        range: std::ops::Range<usize>,
    ) -> Result<usize> {
        let mut skip_until = 0;
        match event {
            pulldown_cmark::Event::Start(pulldown_cmark::Tag::FootnoteDefinition(label)) => {
                self.in_footnote_definition = true;
                self.current_footnote_definition_start = range.start;
                self.current_footnote_definition_label = label;
                self.level += 1;
            }
            pulldown_cmark::Event::End(pulldown_cmark::Tag::FootnoteDefinition(_)) => {
                // Pandoc supports multiline footnote definitions, pulldown-cmark doesn't.
                let end = scan_multiline_footnote(md, range.end);
                skip_until = end;

                self.footnote_definitions.insert(
                    std::mem::replace(
                        &mut self.current_footnote_definition_label,
                        pulldown_cmark::CowStr::Borrowed(""),
                    ),
                    self.current_footnote_definition_start..end,
                );
                self.in_footnote_definition = false;
                self.level -= 1;
            }

            pulldown_cmark::Event::Start(_) => {
                self.level += 1;
            }
            pulldown_cmark::Event::End(tag) => {
                self.level -= 1;

                if let pulldown_cmark::Tag::Link(pulldown_cmark::LinkType::Reference, _, _)
                | pulldown_cmark::Tag::Link(pulldown_cmark::LinkType::Shortcut, _, _) = tag
                {
                    // TODO: ensure last is "]"
                    let mut end = range.end;
                    loop {
                        end = range.start
                            + md[range.start..end]
                                .rfind('[')
                                .context("Missing [ in reference")?;
                        // label can contain ], it just has to be escaped as \]
                        if end == 0 || !&md[end - 1..].starts_with('\\') {
                            break;
                        }
                    }
                    let label = &md[end + 1..range.end - 1];
                    self.current_block_link_references.push(label);
                }

                if self.level == 0 {
                    // Don't skip over whitespace at beginning of block
                    // Important for indented code blocks
                    // https://pandoc.org/MANUAL.html#indented-code-blocks
                    let range = if let pulldown_cmark::Tag::CodeBlock(
                        pulldown_cmark::CodeBlockKind::Indented,
                    ) = tag
                    {
                        md[..range.start]
                            .rfind('\n')
                            .map(|index| index + 1..range.end)
                            .unwrap_or(range)
                    } else {
                        range
                    };

                    self.blocks.push(ParsedBlock {
                        footnote_references: std::mem::take(
                            &mut self.current_block_footnote_references,
                        ),
                        link_references: std::mem::take(&mut self.current_block_link_references),
                        range,
                    });
                }
            }

            pulldown_cmark::Event::FootnoteReference(label) => {
                if self.in_footnote_definition {
                    // Pandoc compatibility: pandoc allows footnote definitions separated by \n, pulldown-cmark needs \n\n.
                    // So for pulldown-cmark, something like this
                    // [^1]: Footnote 1
                    // [^2]: Footnote 2
                    // looks like a single footnote definition for ^1 that references footnote ^2. But for us it should be
                    // two footnote definitions.
                    // Pandoc does not allow one footnote referencing another one, so this won't accidentally introduce a
                    // different compatibility issue here.
                    self.footnote_definitions.insert(
                        std::mem::replace(&mut self.current_footnote_definition_label, label),
                        self.current_footnote_definition_start..range.start - 1,
                    );
                    self.current_footnote_definition_start = range.start;
                } else {
                    // Normal footnote reference
                    self.current_block_footnote_references.push(label.clone());
                }
            }

            pulldown_cmark::Event::Rule if self.level == 0 => {
                // A Rule may indicate a yaml metadata block
                if let Some((yaml_end, parsed_yaml)) =
                    try_parse_yaml_metadata_block(md, range.start)
                {
                    if let Some(ref mut metadata) = self.metadata {
                        // A second/third/... metadata block. Merge it into the existing one
                        for (key, value) in parsed_yaml.into_iter() {
                            metadata.insert(key, value);
                        }
                    } else {
                        // First metadata block, use it directly
                        self.metadata = Some(parsed_yaml);
                    }
                    skip_until = yaml_end;
                } else {
                    // A top-level Rule
                    self.blocks.push(ParsedBlock {
                        footnote_references: Vec::new(),
                        link_references: Vec::new(),
                        range,
                    });
                }
            }

            _ => {
                if self.level == 0 {
                    // A single-event top-level block, e.g. a Rule
                    // TODO: can this happen actually? Rule is handled separately now...
                    self.blocks.push(ParsedBlock {
                        footnote_references: Vec::new(),
                        link_references: Vec::new(),
                        range,
                    });
                }
            }
        }

        // TODO: save source locations, at least at block-level? -- maybe we can use these to
        //       implement "jump to block" in editor?

        // TODO: pmpm right now = references to same footnote in different blocks = footnote appears
        //       twice. Here, we maybe can improve on this since we anyway parse the thing?

        // TODO: referencing footnotes in `title: ...` does not work (also doesn't work in normal pmpm)

        Ok(skip_until)
    }

    fn finalize(self) -> Result<SplitMarkdown> {
        let blocks = self
            .blocks
            .iter()
            .map(|block| {
                let length = (block.range.end - block.range.start)
                    + 1
                    + block
                        .link_references
                        .iter()
                        .map(|label| {
                            self.link_reference_definitions
                                .get(*label)
                                .map_or(0, |range| range.end - range.start + 1)
                        })
                        .sum::<usize>()
                    + block
                        .footnote_references
                        .iter()
                        .map(|label| {
                            self.footnote_definitions
                                .get(label)
                                .map_or(0, |range| range.end - range.start + 2)
                        })
                        .sum::<usize>();

                let mut buf = String::with_capacity(length);
                // block content
                writeln!(buf, "{}", &self.md[block.range.start..block.range.end])?;
                // add definitions
                for label in &block.link_references {
                    // don't just unwrap in case of missing definition (common while typing!)
                    if let Some(range) = self.link_reference_definitions.get(*label) {
                        writeln!(buf, "{}", &self.md[range.start..range.end])?;
                    }
                }
                // add footnotes
                for label in &block.footnote_references {
                    // don't just unwrap in case of missing definition (common while typing!)
                    if let Some(range) = self.footnote_definitions.get(label) {
                        let def = &self.md[range.start..range.end];
                        write!(buf, "{}", def)?;
                        if !def.ends_with('\n') {
                            write!(buf, "\n\n")?;
                        } else if !def.ends_with("\n\n") {
                            writeln!(buf)?;
                        }
                    }
                }

                Ok(buf)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(SplitMarkdown {
            metadata: self.metadata.unwrap_or_default(),
            blocks,
            titleblock: self.titleblock,
        })
    }
}

#[cached(
    result = true,
    size = 10,
    key = "u64",
    convert = "{
    let mut hasher = DefaultHasher::new();
    md.hash(&mut hasher);
    hasher.finish()
}"
)]
pub async fn md2mdblocks(md: &str) -> Result<SplitMarkdown> {
    let mut splitter = Splitter::new(md);
    splitter.split()?;
    splitter.finalize()
}

#[cfg(test)]
mod tests {

    use crate::md_cmark::split_md::*;

    use std::path::PathBuf;

    use bytes::Bytes;

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
    async fn metadata_block_at_start_with_one_newline() -> Result<()> {
        let md = "\n---\ntoc: true\n---\n";
        assert_eq!(
            md2mdblocks(md).await?.metadata.get("toc"),
            Some(&serde_yaml::Value::Bool(true))
        );

        let md = "\n\n---\ntoc: true\n---\n";
        assert_eq!(
            md2mdblocks(md).await?.metadata.get("toc"),
            Some(&serde_yaml::Value::Bool(true))
        );

        let md = "\nasdf\n---\ntoc: true\n---\n";
        assert_eq!(md2mdblocks(md).await?.metadata.get("toc"), None);

        let md = "asdf\n---\ntoc: true\n---\n";
        assert_eq!(md2mdblocks(md).await?.metadata.get("toc"), None);

        let md = "\n---\ntoc: true\n---";
        assert_eq!(
            md2mdblocks(md).await?.metadata.get("toc"),
            Some(&serde_yaml::Value::Bool(true))
        );

        let md = "\n---\ntoc: true\n---\n\nasdf";
        let split_md = md2mdblocks(md).await?;
        assert_eq!(
            split_md.metadata.get("toc"),
            Some(&serde_yaml::Value::Bool(true))
        );
        assert_eq!(split_md.blocks.len(), 1);
        assert_eq!(split_md.blocks[0], "asdf\n");

        Ok(())
    }

    #[tokio::test]
    async fn titleblock() -> Result<()> {
        let md = "%asdf1\n asdf2\n asdf3\nasdf4";
        let split_md = md2mdblocks(md).await?;
        assert_eq!(
            split_md.titleblock,
            Some("%asdf1\n asdf2\n asdf3".to_string())
        );
        assert_eq!(split_md.blocks.len(), 1);
        assert_eq!(split_md.blocks[0], "asdf4\n");
        assert_eq!(split_md.metadata.len(), 0);

        let md = "%asdf1\n asdf2";
        assert_eq!(
            md2mdblocks(md).await?.titleblock,
            Some("%asdf1\n asdf2".to_string())
        );

        let md = " %asdf1\n asdf2";
        assert_eq!(md2mdblocks(md).await?.titleblock, None);

        let md = "asdf\n%asdf1\n asdf2";
        assert_eq!(md2mdblocks(md).await?.titleblock, None);

        Ok(())
    }

    #[tokio::test]
    async fn footnote_links() -> Result<()> {
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
    async fn footnote_multiline_def() -> Result<()> {
        let (md, _) = read_file("footnote-defs-multiline.md")?;
        let md = std::str::from_utf8(&md)?;
        let split = md2mdblocks(md).await?;
        assert_eq!(split.blocks.len(), 4);
        assert_eq!(
            split.blocks[0],
            indoc::indoc! {"
            Here is a footnote reference,[^1] and another.[^longnote]

            [^1]: Here is the footnote.

            [^longnote]: Here's one with multiple blocks.

                Subsequent paragraphs are indented to show that they
            belong to the previous footnote.

                    { some.code }

                The whole paragraph can be indented, or just the first
                line.  In this way, multi-paragraph footnotes work like
                multi-paragraph list items.

            "}
        );
        assert_eq!(
            split.blocks[1],
            "This paragraph won't be part of the note, because it\nisn't indented.\n\n"
        );
        assert_eq!(
            split.blocks[2],
            indoc::indoc! {"
            This is the next test [^2] and [^secondlongnote]

            [^2]: Another small footnote

            [^secondlongnote]: Followed without blank line

                by multiline note

            "}
        );
        assert_eq!(
            split.blocks[3],
            "and another paragraph that doesn't belong to the footnote.\n\n"
        );
        Ok(())
    }

    #[tokio::test]
    async fn multiple_metablocks() -> Result<()> {
        let (md, _) = read_file("multiple-metablocks.md")?;
        let md = std::str::from_utf8(&md)?;
        let split = md2mdblocks(md).await?;
        assert_eq!(split.blocks.len(), 6);
        assert_eq!(split.metadata.len(), 4);
        assert_eq!(
            split.metadata.get("title"),
            Some(&serde_yaml::Value::String("my title".to_string()))
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore] // TODO: Stop ignoring once fix is implemented
    async fn list_item_with_gap() -> Result<()> {
        let (md, _) = read_file("list-item-with-gap.md")?;
        let md = std::str::from_utf8(&md)?;
        let split = md2mdblocks(md).await?;
        // TODO: Fix this
        //
        //       Note: Parsing this exactly like pandoc may be a bit tricky (why does
        //       the third code block terminate the list but not the other two? -> actually it's
        //       easy -- it's just the blank line! w/o blank line it's fine w/o indentation --
        //       otherwise must be indented. Same rule with footnote continuation!)
        //
        //       BUT: We can just err on the side of concatenating a few too many blocks!
        //
        //       Because if we fail to split a few blocks that's at most a perf problem.
        //       Correctness will not be affected in this case. And the perf problem is
        //       probably very minor if we only concat a few small blocks.
        //
        //       So we can probably get away with a simple rule like "continue list as
        //       long as the following level-0 tags or standalone blocks are indented".
        assert_eq!(split.blocks.len(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn indented_codeblock() -> Result<()> {
        let (md, _) = read_file("indented-codeblock.md")?;
        let md = std::str::from_utf8(&md)?;
        let split = md2mdblocks(md).await?;
        assert_eq!(split.blocks.len(), 3);
        assert_eq!(split.blocks[0], "Hi\n\n");
        assert_eq!(
            split.blocks[1],
            "    if (a > 3) {\n        moveShip(5 * gravity, DOWN);\n    }\n\n"
        );
        assert_eq!(split.blocks[2], "Ho\n\n");
        Ok(())
    }

    #[tokio::test]
    async fn footnote_defs_without_blank_line() -> Result<()> {
        let (md, _) = read_file("footnote-defs-without-blank-line.md")?;
        let md = std::str::from_utf8(&md)?;
        let split = md2mdblocks(md).await?;
        assert_eq!(split.blocks.len(), 2);
        assert_eq!(split.blocks[0], "Hi [^1]\n\n[^1]: footnote 1\n\n");
        assert_eq!(split.blocks[1], "Ho [^2]\n\n[^2]: footnote 2\n\n");
        Ok(())
    }
}