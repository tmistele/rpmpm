use anyhow::{Context, Result};

use std::collections::hash_map::DefaultHasher;
use std::fmt::Write as _;
use std::hash::{Hash, Hasher};

use cached::proc_macro::cached;

use pulldown_cmark::{CowStr, Event, Tag};

struct ParsedBlock<'a> {
    footnote_references: Vec<CowStr<'a>>,
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

struct ScannedListContinuation {
    unindented_md: String,
    space_deletion_points: Vec<(usize, usize)>,
}

struct ScannedListEnd {
    end: usize,
    continuation: Option<ScannedListContinuation>,
}

// TODO: This works only with list symbols supported by pulldown-cmark, i.e. -*+ and 1., 2., etc.
//       But pandoc also supports a-z, A-Z, roman (i., ii., etc.), ...?

fn scan_list_continuation(md: &str, last_li_start: usize, end: usize) -> Result<ScannedListEnd> {
    // First find out if the last item was an empty one (blank up to the itemize symbol -*+ or [0-9].)
    // Also note the indentation + length of item symbol of the last list item for later.
    // e.g. for a line " -" it's 2, for a line like "10." it's 3. That's the minimum indenation later.

    // Find indent + check for valid list symbol
    let mut symbol_end = 0;
    for (i, c) in md[last_li_start..].bytes().enumerate() {
        match c {
            b'-' => {
                symbol_end = i;
                break;
            }
            b'*' => {
                symbol_end = i;
                break;
            }
            b'+' => {
                symbol_end = i;
                break;
            }
            b'0'..=b'9' => {
                symbol_end = i;
                // no break!
            }
            b'.' => {
                // '.' must come after 0-9
                if symbol_end > 0 {
                    symbol_end = i;
                    break;
                } else {
                    anyhow::bail!("Invalid list item");
                }
            }
            b' ' => {
                // spaces must come before list symbol
                if symbol_end > 0 {
                    anyhow::bail!("Invalid list item");
                }
            }
            _ => anyhow::bail!("Invalid list item"),
        }
    }

    // We're at end of md / list
    if symbol_end == md[last_li_start..end].len() - 1 {
        return Ok(ScannedListEnd {
            end,
            continuation: None,
        });
    }

    let mut blank_lines_before_loop = 0;

    // Check if it's an empty item
    for (_, c) in md[last_li_start + symbol_end + 1..end].bytes().enumerate() {
        if c == b'\n' {
            blank_lines_before_loop += 1;
        } else if c != b' ' {
            // Not an empty item, no need to do anything
            return Ok(ScannedListEnd {
                end,
                continuation: None,
            });
        }
    }

    // Subsequent lines must be indented by at least one more than `symbol_end
    let required_indent = symbol_end + 1;

    // Check how far the block continues.
    let mut pos = end;
    let mut unindented_md = String::new();
    let mut space_deletion_points = Vec::new();

    // Scan until first unindented/underindented line after a purely whitespace line
    let mut prev_line_blank = blank_lines_before_loop >= 2;
    loop {
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
            if prev_line_blank && indent < required_indent {
                // The current line does no longer belong to the footnote
                break;
            }
            prev_line_blank = false;
        }

        if let Some(nl) = nl {
            if blank || indent < required_indent {
                unindented_md.push_str(&md[pos..pos + nl + 1]);
            } else {
                space_deletion_points.push((pos, required_indent));
                unindented_md.push_str(&md[pos + required_indent..pos + nl + 1]);
            }

            // Go to next \n
            pos += nl;
        } else {
            let nl = md[pos..].len();

            if blank || indent < required_indent {
                unindented_md.push_str(&md[pos..pos + nl]);
            } else {
                space_deletion_points.push((pos, required_indent));
                unindented_md.push_str(&md[pos + required_indent..pos + nl]);
            }

            // We reached EOF (didn't find another \n)
            pos += nl;
            break;
        }

        // At position `pos`, we have a \n. Ignore it.
        if md.len() > pos {
            pos += 1;
        } else {
            break;
        }
    }

    return Ok(ScannedListEnd {
        end: pos,
        continuation: if pos > end {
            Some(ScannedListContinuation {
                unindented_md,
                space_deletion_points,
            })
        } else {
            None
        },
    });
}

struct ScannedMultilineFootnoteContinuation {
    unindented_md: String,
    space_deletion_points: Vec<(usize, usize)>,
}

struct ScannedMultilineFootnote {
    end: usize,
    base_indent: usize,
    continuation: Option<ScannedMultilineFootnoteContinuation>,
}

fn scan_multiline_footnote(
    md: &str,
    end: usize,
    base_indent: usize,
) -> Result<ScannedMultilineFootnote> {
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
    if indent < 4 + base_indent {
        return Ok(ScannedMultilineFootnote {
            end,
            base_indent,
            continuation: None,
        });
    }

    let mut unindented_md = String::new();
    let mut space_deletion_points = Vec::new();

    // This indented line belongs to the footnote definition
    pos += if let Some(line) = md[pos..].find('\n') {
        unindented_md.push_str(&md[end + base_indent + 4..pos + line + 1]);
        space_deletion_points.push((end, base_indent + 4));
        line
    } else {
        let line = md.len();
        unindented_md.push_str(&md[end + base_indent + 4..pos + line]);
        space_deletion_points.push((end, base_indent + 4));
        return Ok(ScannedMultilineFootnote {
            end: line,
            base_indent,
            continuation: Some(ScannedMultilineFootnoteContinuation {
                unindented_md,
                space_deletion_points,
            }),
        });
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
            if indent < 4 + base_indent && prev_line_blank {
                // The current line does no longer belong to the footnote
                break;
            }
            prev_line_blank = false;
        }

        if let Some(nl) = nl {
            if blank || indent < 4 + base_indent {
                // We keep blank + underindented lines literally
                // Note that underindented lines only happen after non-blank lines where
                // pandoc does not care about indentation (i.e. indented stuff will not
                // accidentally become an indented CodeBlock or something like that).
                unindented_md.push_str(&md[pos..pos + nl + 1]);
            } else {
                space_deletion_points.push((pos, base_indent + 4));
                unindented_md.push_str(&md[pos + base_indent + 4..pos + nl + 1]);
            }

            // Go to next \n
            pos += nl;
        } else {
            let nl = md[pos..].len();

            if blank || indent < 4 + base_indent {
                unindented_md.push_str(&md[pos..pos + nl]);
            } else {
                space_deletion_points.push((pos, base_indent + 4));
                unindented_md.push_str(&md[pos + base_indent + 4..pos + nl]);
            }

            // We reached EOF (didn't find another \n)
            pos += nl;
            break;
        }
    }

    Ok(ScannedMultilineFootnote {
        end: pos,
        base_indent,
        continuation: Some(ScannedMultilineFootnoteContinuation {
            unindented_md,
            space_deletion_points,
        }),
    })
}

fn link_reference_label_from_range(md: &str, range: std::ops::Range<usize>) -> Result<&str> {
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
    Ok(&md[end + 1..range.end - 1])
}

struct ParsedFootnote<'input> {
    range: std::ops::Range<usize>,
    link_references: Vec<&'input str>,
}

struct Splitter<'input> {
    md: &'input str,
    offset_iter: pulldown_cmark::OffsetIter<'input, 'input>,
    titleblock: Option<String>,
    link_reference_definitions: std::collections::HashMap<String, std::ops::Range<usize>>,
    metadata: Option<Metadata>,
    blocks: Vec<ParsedBlock<'input>>,
    current_block_start: Option<usize>,
    current_block_footnote_references: Vec<CowStr<'input>>,
    current_block_link_references: Vec<&'input str>,
    level: i32,
    footnote_definitions: std::collections::HashMap<CowStr<'input>, ParsedFootnote<'input>>,
    last_li_start: usize,
}

impl<'input> Splitter<'input> {
    fn new(md: &'input str) -> Self {
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
            current_block_start: None,
            current_block_footnote_references: Vec::new(),
            current_block_link_references: Vec::new(),
            level: 0,
            footnote_definitions: std::collections::HashMap::new(),
            last_li_start: 0,
        }
    }

    fn split(&mut self) -> Result<()> {
        let mut skip_until = 0;
        while let Some((event, range)) = self.offset_iter.next() {
            // Skip e.g. yaml metadata block we parsed ourselves
            if range.start < skip_until {
                continue;
            }
            skip_until = self.step(event, range)?;
        }
        Ok(())
    }

    fn scan_and_parse_footnote(
        &mut self,
        mut label: CowStr<'input>,
        range: std::ops::Range<usize>,
    ) -> Result<usize> {
        // First handle the normal non-multiline stuff
        let mut link_references = Vec::new();
        let mut start = range.start;
        while let Some((event, range)) = self.offset_iter.next() {
            match event {
                Event::End(Tag::Link(pulldown_cmark::LinkType::Reference, _, _))
                | Event::End(Tag::Link(pulldown_cmark::LinkType::Shortcut, _, _)) => {
                    let label = link_reference_label_from_range(self.md, range)?;
                    link_references.push(label);
                }

                Event::FootnoteReference(ref_label) => {
                    // Pandoc compatibility: pandoc allows footnote definitions separated by \n, pulldown-cmark needs \n\n.
                    // So for pulldown-cmark, something like this
                    // [^1]: Footnote 1
                    // [^2]: Footnote 2
                    // looks like a single footnote definition for ^1 that references footnote ^2. But for us it should be
                    // two footnote definitions.
                    // Pandoc does not allow one footnote referencing another one, so this won't accidentally introduce a
                    // different compatibility issue here.
                    self.footnote_definitions.insert(
                        std::mem::replace(&mut label, ref_label),
                        ParsedFootnote {
                            range: start..range.start - 1,
                            link_references: std::mem::replace(&mut link_references, Vec::new()),
                        },
                    );
                    start = range.start;
                }

                Event::End(Tag::FootnoteDefinition(_)) => {
                    break;
                }
                _ => {}
            }
        }
        let range = start..range.end;

        // Find out how long this footnote block actually is
        let multiline_fn = scan_multiline_footnote(self.md, range.end, 0)?;

        if multiline_fn.continuation.is_none() {
            // Just a single-line footnote
            self.footnote_definitions.insert(
                label,
                ParsedFootnote {
                    range,
                    link_references,
                },
            );
        } else {
            // It's a multiline footnote
            self.footnote_multiline_continuation(
                &multiline_fn,
                range,
                None,
                label,
                link_references,
            )?;
        }

        self.level -= 1;

        Ok(multiline_fn.end)
    }

    fn footnote_multiline_continuation(
        &mut self,
        multiline_fn: &ScannedMultilineFootnote,
        range_firstline: std::ops::Range<usize>,
        range_outer: Option<&std::ops::Range<usize>>,
        label: CowStr<'input>,
        mut link_references: Vec<&'input str>,
    ) -> Result<()> {
        let continuation = multiline_fn.continuation.as_ref().unwrap();
        let cont_md = &continuation.unindented_md;
        let space_deletion_points = &continuation.space_deletion_points;

        // Translates from a range in `cont_md` to a range in the original `self.md`
        let translate_cont_range_to_orig_md = |cont_range: &std::ops::Range<usize>| {
            let mut start = range_firstline.end + cont_range.start;
            let mut end = range_firstline.end + cont_range.end;
            for (deletion_point, ndel) in space_deletion_points.iter() {
                if start > *deletion_point {
                    start += ndel;
                    end += ndel;
                } else if end > *deletion_point {
                    end += ndel;
                } else {
                    // Small perf optimization
                    break;
                }
            }
            start..end
        };

        let mut options = pulldown_cmark::Options::empty();
        options.insert(pulldown_cmark::Options::ENABLE_FOOTNOTES);

        let mut broken_link_callback = |_broken: pulldown_cmark::BrokenLink| {
            // pretend every reference exists.
            // If that's wrong, nothing bad happens, pandoc will notice later
            Some((CowStr::Borrowed(""), CowStr::Borrowed("")))
        };
        let parser = pulldown_cmark::Parser::new_with_broken_link_callback(
            &cont_md,
            options,
            Some(&mut broken_link_callback),
        );

        // TODO: new reference_definitions? Is this possible within multiline footnote?

        // For all nested footnotes, we push the *whole* footnote block to `self.footnote_definitions`.
        // That is, for the footnote [^nested] in this definition
        //
        // [^outer]: Outer
        //
        //    [^nested]: Inner
        //
        //
        // We push the whole thing to `self.footnote_definitions`, including `[^outer]`.
        // This is because otherwise, blocks referencing [^nested] see this footnote definition
        //
        //     [^nested]
        //
        // But without the enclosing [^outer] definition, this is actually an indented code block
        // for pandoc, so it will not work!
        //
        // We could try to unindent the footnote, but then we can no longer just push `Range`s to
        // `self.footnote_definitions`, which makes things more complicated.
        //
        // Just always pushing the outer block is of course not optimal for perf, but I think nested
        // footnotes are uncommon, so it doesn't matter much.
        //
        // Note that this will lead to multiple definitions in blocks that reference both [^outer]
        // and [^inner]. The same whole block will be pasted twice at the end (in `self.finalize()`).
        // But it seems this is fine for pandoc, so it's also fine for us.

        let mut in_nested_footnote = false;
        let mut nested_label = CowStr::Borrowed("");
        let mut nested_link_references = Vec::new();

        let mut skip_until = 0;
        let mut fake_offset_iter = parser.into_offset_iter();
        while let Some((cont_event, cont_range)) = fake_offset_iter.next() {
            let range_orig = translate_cont_range_to_orig_md(&cont_range);
            if range_orig.start < skip_until {
                continue;
            }

            match cont_event {
                // TODO: pandoc allows an indented yaml metadata block inside multiline footnote. Handle + skip it!
                Event::End(Tag::Link(pulldown_cmark::LinkType::Reference, _, _))
                | Event::End(Tag::Link(pulldown_cmark::LinkType::Shortcut, _, _))
                | Event::End(Tag::Link(pulldown_cmark::LinkType::ShortcutUnknown, _, _)) => {
                    let label = link_reference_label_from_range(self.md, range_orig)?;
                    if in_nested_footnote {
                        nested_link_references.push(label);
                    } else {
                        link_references.push(label);
                    }
                }

                Event::Start(Tag::FootnoteDefinition(label)) => {
                    self.level += 1;
                    in_nested_footnote = true;

                    // We need `nested_label` to reference `self.md` not `cont_md`
                    let label_pos = range_orig.start
                        + self.md[range_orig.start..range_orig.end]
                            .find(label.as_ref())
                            .context("label not found")?;
                    let label = &self.md[label_pos..label_pos + label.len()];
                    nested_label = CowStr::Borrowed(label);
                }

                // TODO: Add test for this FootnoteReference thing in nested multiline ...
                Event::FootnoteReference(ref_label) => {
                    in_nested_footnote = true;
                    // We need `ref_label` to reference `self.md` not `cont_md`
                    let ref_label_pos = range_orig.start
                        + self.md[range_orig.start..range_orig.end]
                            .find(ref_label.as_ref())
                            .context("label not found")?;
                    let ref_label = &self.md[ref_label_pos..ref_label_pos + ref_label.len()];

                    // Pandoc compatibility:
                    // A `FootnoteReference` inside a footnote is actually a new footnote definition.
                    // See comment in `scan_and_parse_footnote` for more details
                    self.footnote_definitions.insert(
                        std::mem::replace(&mut nested_label, CowStr::Borrowed(ref_label)),
                        ParsedFootnote {
                            range: range_outer.map_or_else(
                                || range_firstline.start..multiline_fn.end,
                                |ro| ro.clone(),
                            ),
                            link_references: std::mem::replace(
                                &mut nested_link_references,
                                Vec::new(),
                            ),
                        },
                    );
                }

                Event::End(Tag::FootnoteDefinition(_)) => {
                    // Find out how long this footnote block actually is
                    let nested_multiline_fn = scan_multiline_footnote(
                        self.md,
                        range_orig.end,
                        multiline_fn.base_indent + 4,
                    )?;

                    let immediate_parent_range = range_firstline.start..multiline_fn.end;

                    if nested_multiline_fn.continuation.is_none() {
                        // Just a single-line footnote
                        self.footnote_definitions.insert(
                            std::mem::replace(&mut nested_label, CowStr::Borrowed("")),
                            ParsedFootnote {
                                range: range_outer
                                    .map_or_else(|| immediate_parent_range, |ro| ro.clone()),
                                link_references: std::mem::replace(
                                    &mut nested_link_references,
                                    Vec::new(),
                                ),
                            },
                        );
                    } else {
                        // It's a multiline footnote
                        self.footnote_multiline_continuation(
                            &nested_multiline_fn,
                            range_orig,
                            range_outer.or_else(|| Some(&immediate_parent_range)),
                            std::mem::replace(&mut nested_label, CowStr::Borrowed("")),
                            std::mem::replace(&mut nested_link_references, Vec::new()),
                        )?;
                    }

                    self.level -= 1;
                    skip_until = nested_multiline_fn.end;
                    in_nested_footnote = false;
                }

                _ => {}
            };
        }

        self.footnote_definitions.insert(
            label,
            ParsedFootnote {
                range: range_outer
                    .map_or_else(|| range_firstline.start..multiline_fn.end, |ro| ro.clone()),
                link_references,
            },
        );

        Ok(())
    }

    fn step(&mut self, event: Event<'input>, range: std::ops::Range<usize>) -> Result<usize> {
        let md = self.md;
        let mut skip_until = 0;
        match event {
            Event::Start(Tag::FootnoteDefinition(label)) => {
                self.level += 1;
                skip_until = self.scan_and_parse_footnote(label, range)?;
            }
            Event::Start(Tag::Item) => {
                self.level += 1;
                self.last_li_start = range.start;
            }
            Event::Start(tag) => {
                // Set start of range of current block.
                // Don't do this if we're in a block that's just being continued (see e.g. list continuation)
                if self.level == 0 && self.current_block_start.is_none() {
                    // Don't skip over whitespace at beginning of block
                    // Important for indented code blocks
                    // https://pandoc.org/MANUAL.html#indented-code-blocks
                    let start = if let Tag::CodeBlock(pulldown_cmark::CodeBlockKind::Indented) = tag
                    {
                        md[..range.start]
                            .rfind('\n')
                            .map(|index| index + 1)
                            .unwrap_or(range.start)
                    } else {
                        range.start
                    };

                    self.current_block_start = Some(start);
                }

                self.level += 1;
            }
            Event::End(tag) => {
                self.level -= 1;

                if let Tag::Link(pulldown_cmark::LinkType::Reference, _, _)
                | Tag::Link(pulldown_cmark::LinkType::Shortcut, _, _) = tag
                {
                    let label = link_reference_label_from_range(self.md, range.clone())?;
                    self.current_block_link_references.push(label);
                }

                if self.level == 0 {
                    // Pandoc sometimes continues a list when pulldown-cmark starts
                    // a new block. Detect and handle this.
                    let range = if let Tag::List(_) = tag {
                        // TODO: keep track of used links inside the skipped block, something like `self.scan_and_parse_footnote()`
                        // TODO: nested list continuation?
                        // TODO: keep track of footnote definitions within nested list (can this actually happen?)
                        // TODO: yaml metadata block inside?
                        let scanned_list_end =
                            scan_list_continuation(self.md, self.last_li_start, range.end)?;
                        if scanned_list_end.continuation.is_some() {
                            skip_until = scanned_list_end.end;
                            if skip_until < md.len() {
                                // Avoid adding block.
                                // This continued list will be concatenated together with the next block.
                                // Note: The next block may or may not be a continuation of the current list.
                                // So sometimes we make one block out of what should be two blocks.
                                // This may hurt performance a bit, but it's not a correctness problem!
                                return Ok(skip_until);
                            } else {
                                // If we skip until the end of the block, add the current block (and then skip in the normal way)
                                range.start..skip_until
                            }
                        } else {
                            range
                        }
                    } else {
                        range
                    };

                    self.blocks.push(ParsedBlock {
                        footnote_references: std::mem::take(
                            &mut self.current_block_footnote_references,
                        ),
                        link_references: std::mem::take(&mut self.current_block_link_references),
                        range: self.current_block_start.take().context("no block start")?
                            ..range.end,
                    });
                }
            }

            Event::FootnoteReference(label) => {
                self.current_block_footnote_references.push(label);
            }

            Event::Rule if self.level == 0 => {
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
                            self.footnote_definitions.get(label).map_or(0, |fnt| {
                                fnt.link_references
                                    .iter()
                                    .map(|label| {
                                        self.link_reference_definitions
                                            .get(*label)
                                            .map_or(0, |range| range.end - range.start + 1)
                                    })
                                    .sum::<usize>()
                                    + fnt.range.end
                                    - fnt.range.start
                                    + 2
                            })
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
                    if let Some(fnt) = self.footnote_definitions.get(label) {
                        // footnote definitions
                        let def = &self.md[fnt.range.start..fnt.range.end];
                        write!(buf, "{}", def)?;
                        if !def.ends_with('\n') {
                            write!(buf, "\n\n")?;
                        } else if !def.ends_with("\n\n") {
                            writeln!(buf)?;
                        }

                        // footnote link references
                        for label in &fnt.link_references {
                            // don't just unwrap in case of missing definition (common while typing!)
                            if let Some(range) = self.link_reference_definitions.get(*label) {
                                writeln!(buf, "{}", &self.md[range.start..range.end])?;
                            }
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
    async fn footnote_with_links() -> Result<()> {
        let (md, _) = read_file("links-in-footnote.md")?;
        let md = std::str::from_utf8(&md)?;
        let split = md2mdblocks(md).await?;

        assert_eq!(split.blocks.len(), 2);
        assert_eq!(
            split.blocks[0],
            indoc::indoc! {"
            Footnote 1[^1]

            [^1]: [link]

            [link]: https://github.com
            "}
        );
        assert_eq!(
            split.blocks[1],
            indoc::indoc! {"
            Footnote 2[^2]

            [^2]: Link in multiline footnote

                works as well [link], right?

            [link]: https://github.com
            "}
        );
        Ok(())
    }

    #[tokio::test]
    async fn footnote_nested_multiline_and_link_refs() -> Result<()> {
        let (md, _) = read_file("footnote-with-link-references-nested-footnote-defs.md")?;
        let md = std::str::from_utf8(&md)?;
        let split = md2mdblocks(md).await?;

        // Note that nested footnote blocks are always appended to md blocks in full
        // i.e. not just the parts that are actually needed, see `Splitter::footnote_multiline_continuation`
        assert_eq!(split.blocks.len(), 2);
        assert_eq!(
            split.blocks[0],
            indoc::indoc! {"
            Here is a footnote reference,[^1] and another.[^longnote] [^longnote2]

            [^1]: Here another footnote using the [gh] link

            [gh]: https://github.com/
            [^longnote]: Here another one

                that uses the link only in a multiline continuation.

                [^nested]: And then we get another footnote

            [^longnote2]: A second long footnote with nested stuff

                second line

                [^longnested]: longnested??

                    longnested cont
                longnested cont2

                    longnested cont3 with link [gh]
            longnested cont4
              longnested cont5

                back to longnote2

            "}
        );
        assert_eq!(
            split.blocks[1],
            indoc::indoc! {"
            Nested ones: [^nested] [^longnested]

            [^longnote]: Here another one

                that uses the link only in a multiline continuation.

                [^nested]: And then we get another footnote

            [^longnote2]: A second long footnote with nested stuff

                second line

                [^longnested]: longnested??

                    longnested cont
                longnested cont2

                    longnested cont3 with link [gh]
            longnested cont4
              longnested cont5

                back to longnote2

            [gh]: https://github.com/
            "}
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
    async fn list_item_with_gap() -> Result<()> {
        let (md, _) = read_file("list-item-with-gap.md")?;
        let md = std::str::from_utf8(&md)?;
        let split = md2mdblocks(md).await?;
        assert_eq!(split.blocks.len(), 2);
        assert_eq!(
            split.blocks[0],
            indoc::indoc! {"
                -

                 test

                -

                  ---

                   test2

                   ```julia
                x = 1
                ```

                    four-space indented

                   test3

                   ```python

                   y=1
                   ```

                   test4

                   ```python

                z=1
                ```

                -


            "}
        );
        assert_eq!(split.blocks[1], "test3\n\n");
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

    #[tokio::test]
    async fn scan_multiline_footnote_basic() -> Result<()> {
        let md = "other\n\n[^1]: asdf";
        let scanned = scan_multiline_footnote(&md, md.len() - 1, 0)?;
        assert_eq!(scanned.end, md.len() - 1);
        assert_eq!(scanned.base_indent, 0);
        assert!(scanned.continuation.is_none());

        let md = "other\n\n[^1]: asdf\n\n    bsdf\n\ncsdf";
        let scanned = scan_multiline_footnote(&md, 19, 0)?;
        assert_eq!(&md[19..scanned.end], "    bsdf\n\n");
        assert_eq!(scanned.base_indent, 0);
        assert!(scanned.continuation.is_some());
        assert_eq!(
            scanned.continuation.as_ref().unwrap().unindented_md,
            "bsdf\n\n"
        );
        assert_eq!(
            scanned.continuation.as_ref().unwrap().space_deletion_points,
            vec![(19, 4)]
        );

        let md = indoc::indoc! {"
            other

            [^1]: asdf

                bsdf

                [^nested]: nested1
                    nested2

                    nested3
                nested4

                    nested5

                csdf
        "};
        let pos = md.find("nested1").unwrap() + 8;
        let scanned = scan_multiline_footnote(&md, pos, 4)?;
        assert_eq!(scanned.base_indent, 4);
        assert_eq!(
            &md[pos..scanned.end],
            "        nested2\n\n        nested3\n    nested4\n\n        nested5\n\n"
        );
        assert_eq!(
            scanned.continuation.as_ref().unwrap().space_deletion_points,
            vec![(pos, 8), (pos + 8 + 9, 8), (pos + 8 + 9 + 8 + 8 + 4 + 9, 8)]
        );
        assert_eq!(
            scanned.continuation.as_ref().unwrap().unindented_md,
            "nested2\n\nnested3\n    nested4\n\nnested5\n\n"
        );
        Ok(())
    }

    fn scan_list_helper(md: &str) -> Result<(usize, ScannedListEnd)> {
        let mut last_li_start = 0;
        let (_, range) = pulldown_cmark::Parser::new(&md)
            .into_offset_iter()
            .find(|(event, range)| match event {
                Event::End(Tag::List(_)) => true,
                Event::End(Tag::Item) => {
                    last_li_start = range.start;
                    false
                }
                _ => false,
            })
            .context("no list end")?;
        Ok((
            range.start,
            scan_list_continuation(&md, last_li_start, range.end)?,
        ))
    }

    fn do_scan_list_continuation(md: &str, expected: &str) -> Result<()> {
        let (list_start, scanned) = scan_list_helper(md)?;
        assert_eq!(&md[list_start..scanned.end], expected);
        Ok(())
    }

    #[tokio::test]
    async fn scan_list_continuation_basic() -> Result<()> {
        do_scan_list_continuation("-\n test", "-\n test")?;
        do_scan_list_continuation("-\n\n test", "-\n\n test")?;
        do_scan_list_continuation("-\n  test", "-\n  test")?;
        do_scan_list_continuation("-\n\n  test", "-\n\n  test")?;
        do_scan_list_continuation(" -\n test", " -\n test")?;
        do_scan_list_continuation(" -\n\n test", " -\n\n")?;
        do_scan_list_continuation(" -\nasdf", " -\nasdf")?;
        do_scan_list_continuation("- asdf\n -\n\n asdf", "- asdf\n -\n\n")?;
        do_scan_list_continuation(" - asdf\n- asdf\n -\n\n asdf", " - asdf\n- asdf\n -\n\n")?;
        do_scan_list_continuation(" - \n\n  asdf", " - \n\n  asdf")?;
        do_scan_list_continuation(" - \n\n   asdf", " - \n\n   asdf")?;
        assert!(do_scan_list_continuation("    -\n\n asdf", "").is_err());
        do_scan_list_continuation("   -\n  asdf\n\n    asdf", "   -\n  asdf\n\n    asdf")?;

        let (_, scanned) = scan_list_helper("-\n test")?;
        assert_eq!(scanned.continuation.as_ref().unwrap().unindented_md, "test");
        assert_eq!(
            scanned.continuation.as_ref().unwrap().space_deletion_points,
            vec![(2, 1)]
        );

        Ok(())
    }
}
