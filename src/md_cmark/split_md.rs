use anyhow::{Context, Result};

use std::collections::{hash_map::DefaultHasher, HashMap};
use std::fmt::Write as _;
use std::hash::{Hash, Hasher};

use cached::proc_macro::cached;

use pulldown_cmark::{CowStr, Event, Tag};

use unicase::UniCase;

struct ParsedBlock<'a> {
    footnote_references: Vec<CowStr<'a>>,
    link_references: Vec<&'a str>,
    range: core::ops::Range<usize>,
}

type Metadata = HashMap<String, serde_yaml::Value>;

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
    // For nested lists, the items don't start at line beginning, but at previous indent level
    // Could in principle save this `rfind()` by keeping track of indent levels?
    let last_li_start = md[..last_li_start].rfind('\n').map_or(0, |x| x + 1);

    // First find out if the last item was an empty one (blank up to the itemize symbol -*+ or [0-9].)
    // Also note the indentation + length of item symbol of the last list item for later.
    // e.g. for a line " -" it's 2, for a line like "10." it's 3. That's the minimum indenation later.

    // Find indent + check for valid list symbol
    let mut symbol_end = None;
    for (i, c) in md[last_li_start..].bytes().enumerate() {
        match c {
            b'-' | b'*' | b'+' => {
                symbol_end = Some(i);
                break;
            }
            b'0'..=b'9' => {
                symbol_end = Some(i);
                // no break!
            }
            b'.' | b')' => {
                // '.' or ')' must come after 0-9
                if symbol_end.is_some() {
                    symbol_end = Some(i);
                    break;
                } else {
                    anyhow::bail!("Invalid list item");
                }
            }
            b' ' => {
                // spaces must come before list symbol
                if symbol_end.is_some() {
                    anyhow::bail!("Invalid list item");
                }
            }
            _ => anyhow::bail!("Invalid list item"),
        }
    }
    let symbol_end = symbol_end.context("no list symbol found")?;

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
                // The current line does no longer belong to the list
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

    Ok(ScannedListEnd {
        end: pos,
        continuation: if pos > end {
            Some(ScannedListContinuation {
                unindented_md,
                space_deletion_points,
            })
        } else {
            None
        },
    })
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

fn find_footnote_indent(md: &str, start: usize) -> Result<usize> {
    let line_start = md[..start].rfind('\n').map_or(0, |x| x + 1);
    let fnt_bracket_start = start + md[start..].find('[').context("missing [")?;
    Ok(fnt_bracket_start - line_start)
}

fn scan_multiline_footnote(
    md: &str,
    range_firstline: &std::ops::Range<usize>,
) -> Result<ScannedMultilineFootnote> {
    let end = range_firstline.end;

    // Find out indentation level
    let base_indent = find_footnote_indent(md, range_firstline.start)?;

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
    indent: usize,
    link_references: Vec<&'input str>,
}

struct Splitter<'input> {
    md: &'input str,
    offset_iter: pulldown_cmark::OffsetIter<'input, 'input>,
    titleblock: Option<String>,
    metadata: Option<Metadata>,
    blocks: Vec<ParsedBlock<'input>>,
    level: i32,
    footnote_definitions: HashMap<CowStr<'input>, ParsedFootnote<'input>>,
    additional_reference_definitions: HashMap<UniCase<CowStr<'input>>, std::ops::Range<usize>>,
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

        let offset_iter = pulldown_cmark::Parser::new_ext(md, options).into_offset_iter();

        Splitter {
            md,
            offset_iter,
            titleblock,
            metadata: None,
            blocks: Vec::new(), // TODO: capacity? guess from last one?
            level: 0,
            footnote_definitions: HashMap::new(),
            additional_reference_definitions: HashMap::new(),
        }
    }

    fn maybe_parse_yaml_metadata_block(&mut self, start: usize) -> Option<usize> {
        let (yaml_end, parsed_yaml) = try_parse_yaml_metadata_block(self.md, start)?;

        if let Some(ref mut metadata) = self.metadata {
            // A second/third/... metadata block. Merge it into the existing one
            for (key, value) in parsed_yaml.into_iter() {
                metadata.insert(key, value);
            }
        } else {
            // First metadata block, use it directly
            self.metadata = Some(parsed_yaml);
        }
        Some(yaml_end)
    }

    fn split(&mut self) -> Result<()> {
        let md = self.md;

        let mut last_li_start = 0;
        let mut current_block_start = None;
        let mut current_block_footnote_references = Vec::new();
        let mut current_block_link_references = Vec::new();

        let mut fn_range: Option<std::ops::Range<usize>> = None;
        let mut fn_label = CowStr::Borrowed("");
        let mut fn_link_references = Vec::new();

        let mut skip_until = 0;
        while let Some((event, range)) = self.offset_iter.next() {
            // Skip e.g. yaml metadata block we parsed ourselves
            if range.start < skip_until {
                // Still keep track of level.
                match event {
                    Event::Start(_) => self.level += 1,
                    Event::End(_) => self.level -= 1,
                    _ => {}
                }
                continue;
            }

            // Add footnote definitions for which we don't get an End(FootnoteDefinition) event.
            // Happens for those that look like a FootnoteReference to pulldown-cmark (see
            // `is_definition` below).
            if let Some(ref fn_range_val) = fn_range {
                if range.start > fn_range_val.end {
                    skip_until = self.footnote_add(
                        fn_range_val.clone(),
                        &mut fn_label,
                        &mut fn_link_references,
                    )?;
                    fn_range = None;
                    if range.start < skip_until {
                        // Still keep track of level.
                        match event {
                            Event::Start(_) => self.level += 1,
                            Event::End(_) => self.level -= 1,
                            _ => {}
                        }
                        continue;
                    }
                }
            }

            match event {
                Event::Start(Tag::FootnoteDefinition(label)) => {
                    self.level += 1;
                    fn_range = Some(range);
                    fn_label = label;
                }
                Event::End(Tag::FootnoteDefinition(_)) => {
                    // Add footnote, scanning for multiline continuation
                    let multiline_fn_end = self.footnote_add(
                        fn_range.take().context("no fn range?")?,
                        &mut fn_label,
                        &mut fn_link_references,
                    )?;
                    skip_until = multiline_fn_end;
                    self.level -= 1;
                }
                Event::Start(Tag::Item) => {
                    self.level += 1;
                    last_li_start = range.start;
                }
                Event::Start(tag) => {
                    // Set start of range of current block.
                    // Don't do this if we're in a block that's just being continued (see e.g. list continuation)
                    if self.level == 0 && current_block_start.is_none() {
                        // Don't skip over whitespace at beginning of block
                        // Important for indented code blocks
                        // https://pandoc.org/MANUAL.html#indented-code-blocks
                        let start =
                            if let Tag::CodeBlock(pulldown_cmark::CodeBlockKind::Indented) = tag {
                                md[..range.start]
                                    .rfind('\n')
                                    .map(|index| index + 1)
                                    .unwrap_or(range.start)
                            } else {
                                range.start
                            };

                        current_block_start = Some(start);
                    }

                    self.level += 1;
                }
                Event::End(tag) => {
                    self.level -= 1;

                    if let Tag::Link(pulldown_cmark::LinkType::Reference, _, _)
                    | Tag::Link(pulldown_cmark::LinkType::Shortcut, _, _) = tag
                    {
                        let label = link_reference_label_from_range(self.md, range.clone())?;
                        if fn_range.is_some() {
                            fn_link_references.push(label);
                        } else {
                            current_block_link_references.push(label);
                        }
                    }

                    // Pandoc sometimes continues a list when pulldown-cmark starts
                    // a new block. Detect and handle this.
                    let range = if let Tag::List(_) = tag {
                        let scanned_list_end =
                            scan_list_continuation(self.md, last_li_start, range.end)?;
                        if scanned_list_end.continuation.is_some() {
                            // We cannot just skip to `scanned_list_end.end` directly because
                            // the skipped parts might e.g. contain link references or footnote
                            // definitions that we might miss (especially if they're 4-space
                            // indented and so look like a CodeBlock to pulldown-cmark).
                            self.list_blank_item_continuation(
                                range,
                                &scanned_list_end,
                                &mut current_block_link_references,
                                &mut current_block_footnote_references,
                            )?;

                            // Do not add the would-be-ending list as a block.
                            // This continued list will be concatenated together with the next block.
                            //
                            // Note: The next block may or may not be a continuation of the current list.
                            // So sometimes we make one block out of what should be two blocks.
                            // This may hurt performance a bit, but it's not a correctness problem!
                            //
                            // Note also: If skip_until lies beyond the last block, it can happen that this
                            // block is never added here. That's why we add any remaining "dangling" below
                            // after the loop over events.
                            skip_until = scanned_list_end.end;
                            continue;
                        } else {
                            range
                        }
                    } else {
                        range
                    };

                    if self.level == 0 {
                        self.blocks.push(ParsedBlock {
                            footnote_references: std::mem::take(
                                &mut current_block_footnote_references,
                            ),
                            link_references: std::mem::take(&mut current_block_link_references),
                            range: current_block_start.take().context("no block start")?..range.end,
                        });
                    }
                }

                Event::FootnoteReference(label) => {
                    if let Some(ref fn_range_val) = fn_range {
                        // Pandoc compatibility: pandoc allows footnote definitions separated by \n, pulldown-cmark needs \n\n.
                        // So for pulldown-cmark, something like this
                        // [^1]: Footnote 1
                        // [^2]: Footnote 2
                        // looks like a single footnote definition for ^1 that references footnote ^2. But for us it should be
                        // two footnote definitions.
                        // Pandoc does not allow one footnote referencing another one, so this won't accidentally introduce a
                        // different compatibility issue here.
                        //
                        // Can be removed (also in self.footnote_multiline_continuation) when this is fixed?
                        // https://github.com/raphlinus/pulldown-cmark/issues/618
                        self.footnote_definitions.insert(
                            std::mem::replace(&mut fn_label, label),
                            ParsedFootnote {
                                range: fn_range_val.start..range.start - 1,
                                indent: find_footnote_indent(md, fn_range_val.start)?,
                                link_references: std::mem::take(&mut fn_link_references),
                            },
                        );
                        fn_range = Some(range.start..fn_range_val.end);
                    } else {
                        // It's actually a footnote definition if followed by ":" and blank before label
                        // for pandoc. pulldown-cmark doesn't recognize it.
                        let is_definition = if self.md[range.end..].starts_with(": ") {
                            self.md[self.md[..range.start].rfind('\n').map_or(0, |x| x + 1)
                                ..range.start]
                                .trim()
                                .is_empty()
                        } else {
                            false
                        };

                        if is_definition {
                            let end = range.end
                                + self.md[range.end..].find('\n').unwrap_or(self.md.len());
                            fn_range = Some(range.start..end);
                            fn_label = label;
                        } else {
                            // Footnote is just referenced in block (the "normal" case)
                            current_block_footnote_references.push(label);
                        }
                    }
                }

                Event::Rule if self.level == 0 => {
                    // A Rule may indicate a yaml metadata block
                    if let Some(yaml_end) = self.maybe_parse_yaml_metadata_block(range.start) {
                        // Skip the parsed block
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
        }

        // Add any remaining "dangling" footnote definitions
        if let Some(fn_range) = fn_range {
            let _ = self.footnote_add(fn_range, &mut fn_label, &mut fn_link_references)?;
        }

        // Add any remaining "dangling" blocks, see list continuation
        if let Some(start) = current_block_start {
            self.blocks.push(ParsedBlock {
                footnote_references: std::mem::take(&mut current_block_footnote_references),
                link_references: std::mem::take(&mut current_block_link_references),
                // I think "daling" blocks happen only when skip_until is set to the end of the block
                range: start..skip_until,
            });
        }

        // TODO: save source locations, at least at block-level? -- maybe we can use these to
        //       implement "jump to block" in editor?

        // TODO: pmpm right now = references to same footnote in different blocks = footnote appears
        //       twice. Here, we maybe can improve on this since we anyway parse the thing?

        // TODO: referencing footnotes in `title: ...` does not work (also doesn't work in normal pmpm)

        Ok(())
    }

    fn list_blank_item_continuation(
        &mut self,
        range_list_cmark: std::ops::Range<usize>,
        scanned_list_end: &ScannedListEnd,
        current_block_link_references: &mut Vec<&'input str>,
        current_block_footnote_references: &mut Vec<CowStr<'input>>,
    ) -> Result<()> {
        let continuation = scanned_list_end.continuation.as_ref().unwrap();
        let cont_md = &continuation.unindented_md;
        let space_deletion_points = &continuation.space_deletion_points;

        let mut nested_footnote_range: Option<std::ops::Range<usize>> = None;
        let mut nested_fn_label = CowStr::Borrowed("");
        let mut nested_fn_link_references = Vec::new();

        let mut options = pulldown_cmark::Options::empty();
        options.insert(pulldown_cmark::Options::ENABLE_FOOTNOTES);

        let mut broken_link_callback = |_broken: pulldown_cmark::BrokenLink| {
            // pretend every reference exists.
            // If that's wrong, nothing bad happens, pandoc will notice later
            Some((CowStr::Borrowed(""), CowStr::Borrowed("")))
        };
        let fake_offset_iter = pulldown_cmark::Parser::new_with_broken_link_callback(
            cont_md,
            options,
            Some(&mut broken_link_callback),
        )
        .into_offset_iter();

        // Remember reference definitions.
        // These might already be in self.offset_iter.reference_definitions()
        // but they also might not. For example, if [link]: https://... is indented
        // by 4 or more spaces, pulldown-cmark might see them as an indented code block.
        // So just add them.
        for (label, link_def) in fake_offset_iter.reference_definitions().iter() {
            let range_orig = Self::translate_cont_range_to_orig_md(
                &range_list_cmark,
                &link_def.span,
                space_deletion_points,
            );
            let key = &self.md[range_orig.start + 1..range_orig.start + 1 + label.len()];
            let key = UniCase::new(CowStr::Borrowed(key));
            self.additional_reference_definitions
                .insert(key, range_orig);
        }

        let mut last_li_start = 0;

        let mut skip_until = 0;
        for (cont_event, cont_range) in fake_offset_iter {
            let range_orig = Self::translate_cont_range_to_orig_md(
                &range_list_cmark,
                &cont_range,
                space_deletion_points,
            );
            if range_orig.start < skip_until {
                continue;
            }

            // Add footnote definitions for which we don't receive an End(FootnoteDefinition)
            // event. Can happen for footnote definitions that look like a FootnoteReference
            // to pulldown-cmark (see `is_definition` below).
            if let Some(ref nested_range) = nested_footnote_range {
                if range_orig.start > nested_range.end {
                    skip_until = self.footnote_add(
                        nested_range.clone(),
                        &mut nested_fn_label,
                        &mut nested_fn_link_references,
                    )?;
                    nested_footnote_range = None;
                    if range_orig.start < skip_until {
                        continue;
                    }
                }
            }

            match cont_event {
                // TODO: does pandoc allow an indented yaml metadata block inside list?
                Event::End(Tag::Link(pulldown_cmark::LinkType::Reference, _, _))
                | Event::End(Tag::Link(pulldown_cmark::LinkType::Shortcut, _, _))
                | Event::End(Tag::Link(pulldown_cmark::LinkType::ShortcutUnknown, _, _)) => {
                    let label = link_reference_label_from_range(self.md, range_orig)?;
                    if nested_footnote_range.is_some() {
                        nested_fn_link_references.push(label);
                    } else {
                        current_block_link_references.push(label);
                    }
                }

                Event::FootnoteReference(label) => {
                    // We need `label` to reference `self.md` not `cont_md`
                    let label_pos = range_orig.start
                        + self.md[range_orig.start..range_orig.end]
                            .find(label.as_ref())
                            .context("label not found")?;
                    let label = &self.md[label_pos..label_pos + label.len()];

                    // It's actually a footnote definition if followed by ":" and blank before label
                    // for pandoc. But pulldown-cmark just doesn't recognize it.
                    let is_definition = if cont_md[cont_range.end..].starts_with(": ") {
                        cont_md[cont_md[..cont_range.start].rfind('\n').map_or(0, |x| x + 1)
                            ..cont_range.start]
                            .trim()
                            .is_empty()
                    } else {
                        false
                    };

                    if is_definition {
                        let end = range_orig.end
                            + self.md[range_orig.end..]
                                .find('\n')
                                .unwrap_or(self.md.len());
                        nested_footnote_range = Some(range_orig.start..end);
                        nested_fn_label = CowStr::Borrowed(label);
                    } else {
                        current_block_footnote_references.push(CowStr::Borrowed(label));
                    }
                }

                Event::Start(Tag::Item) => {
                    last_li_start = range_orig.start;
                }

                Event::End(Tag::List(_)) => {
                    let scanned_list_end =
                        scan_list_continuation(self.md, last_li_start, range_orig.end)?;
                    if scanned_list_end.continuation.is_some() {
                        // Nested list continuation
                        self.list_blank_item_continuation(
                            range_orig,
                            &scanned_list_end,
                            current_block_link_references,
                            current_block_footnote_references,
                        )?;

                        // Skip the rest of this list, it's parsed in the nested
                        // `list_blank_item_continuation` call
                        skip_until = scanned_list_end.end;
                    }
                }

                _ => {}
            }
        }

        if let Some(nested_range) = nested_footnote_range {
            let _ = self.footnote_add(
                nested_range,
                &mut nested_fn_label,
                &mut nested_fn_link_references,
            )?;
        }

        Ok(())
    }

    fn footnote_add(
        &mut self,
        range_firstline: std::ops::Range<usize>,
        label: &mut CowStr<'input>,
        link_references: &mut Vec<&'input str>,
    ) -> Result<usize> {
        // Find out how long this footnote block actually is
        let multiline_fn = scan_multiline_footnote(self.md, &range_firstline)?;

        if multiline_fn.continuation.is_none() {
            // Just a single-line footnote
            self.footnote_definitions.insert(
                std::mem::replace(label, CowStr::Borrowed("")),
                ParsedFootnote {
                    range: range_firstline,
                    indent: multiline_fn.base_indent,
                    link_references: std::mem::take(link_references),
                },
            );
        } else {
            // It's a multiline footnote
            self.footnote_multiline_continuation(
                &multiline_fn,
                range_firstline,
                std::mem::replace(label, CowStr::Borrowed("")),
                std::mem::take(link_references),
            )?;
        }

        Ok(multiline_fn.end)
    }

    // Translates from a range in `cont_md` to a range in the original `self.md`
    fn translate_cont_range_to_orig_md(
        range_orig_base: &std::ops::Range<usize>,
        cont_range: &std::ops::Range<usize>,
        space_deletion_points: &[(usize, usize)],
    ) -> std::ops::Range<usize> {
        let mut start = range_orig_base.end + cont_range.start;
        let mut end = range_orig_base.end + cont_range.end;
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
    }

    fn footnote_multiline_continuation(
        &mut self,
        multiline_fn: &ScannedMultilineFootnote,
        mut range_firstline: std::ops::Range<usize>,
        label: CowStr<'input>,
        mut link_references: Vec<&'input str>,
    ) -> Result<()> {
        let mut base_indent = multiline_fn.base_indent;
        let continuation = multiline_fn.continuation.as_ref().unwrap();
        let cont_md = &continuation.unindented_md;
        let space_deletion_points = &continuation.space_deletion_points;

        let mut options = pulldown_cmark::Options::empty();
        options.insert(pulldown_cmark::Options::ENABLE_FOOTNOTES);

        let mut broken_link_callback = |_broken: pulldown_cmark::BrokenLink| {
            // pretend every reference exists.
            // If that's wrong, nothing bad happens, pandoc will notice later
            Some((CowStr::Borrowed(""), CowStr::Borrowed("")))
        };
        let fake_offset_iter = pulldown_cmark::Parser::new_with_broken_link_callback(
            cont_md,
            options,
            Some(&mut broken_link_callback),
        )
        .into_offset_iter();

        // TODO: new reference_definitions? Is this possible within multiline footnote?

        // For footnotes containing nested footnotes we include any inner nested footnotes in
        // its range in `self.footnote_definitions`. That is, for the footnote [^outer]
        //
        // [^outer]: Outer
        //
        //    [^nested]: Inner
        //
        // Note that this will lead to multiple definitions in blocks that reference both [^outer]
        // and [^inner]. But it seems this is fine for pandoc, so it's also fine for us.

        let mut in_nested_footnote = false;
        let mut nested_label = CowStr::Borrowed("");
        let mut nested_link_references = Vec::new();

        let mut skip_until = 0;
        for (cont_event, cont_range) in fake_offset_iter {
            let range_orig = Self::translate_cont_range_to_orig_md(
                &range_firstline,
                &cont_range,
                space_deletion_points,
            );
            if range_orig.start < skip_until {
                continue;
            }

            match cont_event {
                // TODO: Detect nested list item with blank first line (i.e. call `self.list_blank_item_continuation()` appropriately)
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
                            range: range_firstline.start..range_orig.start - 1,
                            indent: base_indent,
                            link_references: std::mem::take(&mut nested_link_references),
                        },
                    );
                    // Act as if the original footnote definition started here
                    range_firstline = range_orig.start
                        ..range_orig.end
                            + self.md[range_orig.end..]
                                .find('\n')
                                .unwrap_or(self.md.len());
                    base_indent = find_footnote_indent(self.md, range_orig.start)?;
                }

                Event::End(Tag::FootnoteDefinition(_)) => {
                    let nested_multiline_fn_end = self.footnote_add(
                        range_orig,
                        &mut nested_label,
                        &mut nested_link_references,
                    )?;

                    skip_until = nested_multiline_fn_end;
                    in_nested_footnote = false;
                    self.level -= 1;
                }

                _ => {}
            };
        }

        self.footnote_definitions.insert(
            label,
            ParsedFootnote {
                range: range_firstline.start..multiline_fn.end,
                indent: base_indent,
                link_references,
            },
        );

        Ok(())
    }

    fn finalize(self) -> Result<SplitMarkdown> {
        let ref_defs = self.offset_iter.reference_definitions();

        let get_ref_def_range = |label| {
            ref_defs.get(label).map(|def| &def.span).or_else(|| {
                self.additional_reference_definitions
                    .get(&UniCase::new(label.into()))
            })
        };

        const DIV_DISPLAY_NONE_START: &str = "<div style=\"display: none;\">";
        const DIV_DISPLAY_NONE_END: &str = "</div>";
        const DIV_DISPLAY_NONE_LEN: usize =
            DIV_DISPLAY_NONE_START.len() + DIV_DISPLAY_NONE_END.len();

        let blocks = self
            .blocks
            .iter()
            .map(|block| {
                let block_md = &self.md[block.range.start..block.range.end];
                let block_md_has_newln = block_md.ends_with('\n');

                let length = block_md.len()
                    + if block_md_has_newln { 1 } else { 2 }
                    + block
                        .link_references
                        .iter()
                        .map(|label| {
                            get_ref_def_range(label).map_or(0, |range| range.end - range.start + 1)
                        })
                        .sum::<usize>()
                    + block
                        .footnote_references
                        .iter()
                        .map(|label| {
                            self.footnote_definitions.get(label).map_or(0, |fnt| {
                                let mut length = fnt
                                    .link_references
                                    .iter()
                                    .map(|label| {
                                        get_ref_def_range(label)
                                            .map_or(0, |range| range.end - range.start + 1)
                                    })
                                    .sum::<usize>()
                                    + fnt.range.end
                                    - fnt.range.start
                                    + 2;
                                if fnt.indent >= 4 {
                                    let n = fnt.indent % 4;
                                    length += DIV_DISPLAY_NONE_LEN + 2
                                        + 4*(n*(n+1)/2) // spaces and -
                                        + n // newlines
                                        ;
                                }
                                length
                            })
                        })
                        .sum::<usize>();

                let mut buf = String::with_capacity(length);
                // block content
                if block_md_has_newln {
                    writeln!(buf, "{}", block_md)?;
                } else {
                    writeln!(buf, "{}\n", block_md)?;
                }
                // add definitions
                for label in &block.link_references {
                    // don't just unwrap in case of missing definition (common while typing!)
                    if let Some(range) = get_ref_def_range(label) {
                        writeln!(buf, "{}", &self.md[range.clone()])?;
                    }
                }
                // add footnotes
                for label in &block.footnote_references {
                    // don't just unwrap in case of missing definition (common while typing!)
                    if let Some(fnt) = self.footnote_definitions.get(label) {
                        // footnote definitions

                        // Because multiline footnotes are sensitive to indentation, we preserve
                        // indentation. But if the whole footnote block is indented by >= 4 spaces,
                        // pandoc will then recognize it as a CodeBlock, not a footnote definition.
                        // To avoid this, exploit the fact that pandoc allows footnote definitions
                        // in lists and put them in a fake nested list.
                        let mut indent = 0;
                        while 4 + indent <= fnt.indent {
                            if indent == 0 {
                                writeln!(buf, "{}", DIV_DISPLAY_NONE_START)?;
                            }
                            writeln!(buf, "{}   -", " ".repeat(indent))?;
                            indent += 4;
                        }
                        let line_start =
                            self.md[..fnt.range.start].rfind('\n').map_or(0, |x| x + 1);
                        let def = &self.md[line_start..fnt.range.end];
                        write!(buf, "{}", def)?;
                        if !def.ends_with('\n') {
                            write!(buf, "\n\n")?;
                        } else if !def.ends_with("\n\n") {
                            writeln!(buf)?;
                        }

                        if indent > 0 {
                            writeln!(buf, "{}", DIV_DISPLAY_NONE_END)?;
                        }

                        // footnote link references
                        for label in &fnt.link_references {
                            // don't just unwrap in case of missing definition (common while typing!)
                            if let Some(range) = get_ref_def_range(label) {
                                writeln!(buf, "{}", &self.md[range.clone()])?;
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
        assert_eq!(split_md.blocks[0], "asdf\n\n");

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
        assert_eq!(split_md.blocks[0], "asdf4\n\n");
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

            <div style=\"display: none;\">
               -
                [^nested]: And then we get another footnote

            </div>
            <div style=\"display: none;\">
               -
                [^longnested]: longnested??

                    longnested cont
                longnested cont2

                    longnested cont3 with link [gh]
            longnested cont4
              longnested cont5

            </div>
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
    async fn list_item_with_gap_at_end_with_refdef() -> Result<()> {
        let (md, _) = read_file("list-item-with-gap-at-end-with-refdef.md")?;
        let md = std::str::from_utf8(&md)?;
        let split = md2mdblocks(md).await?;
        assert_eq!(split.blocks.len(), 1);
        assert_eq!(split.blocks[0], "-\n\n test\n\n\n");
        Ok(())
    }

    #[tokio::test]
    async fn list_item_with_gap_and_footnote_and_reference_defs() -> Result<()> {
        let (md, _) = read_file("list-item-with-gap-and-footnote-defs-and-link-defs.md")?;
        let md = std::str::from_utf8(&md)?;
        let split = md2mdblocks(md).await?;

        assert_eq!(split.blocks.len(), 3);

        assert_eq!(
            split.blocks[0],
            // NOTE: The algorithm in `Splitter.list_blank_item_continuation()` concatenates
            //       any list that we manually continue (b/c of a "blank" list item) with the
            //       following block. That's why the "Buffer block" here is in the first block.
            indoc::indoc! {"
                 -

                     test [link]

                     - list inside [^1]
                     - list cont
                     -
                         asdf [liNK2]

                         [^2]: fn 2

                [link]: https://github.com

                [^1]: fn 1

                Buffer block

                [link]: https://github.com
                [LInk2]: https://github.com
                [^1]: fn 1

        "}
        );
        assert_eq!(
            split.blocks[1],
            indoc::indoc! {"
                Here's [^2] another block

                <div style=\"display: none;\">
                   -
                       -
                         [^2]: fn 2

                </div>
        "}
        );
        assert_eq!(
            split.blocks[2],
            indoc::indoc! {"
                - non-blank list item

                    continuation line [^3]

                    [^4]: fn 4

                    -

                        [LInk2]: https://github.com


                [^3]: fn 3

                Here's a [^4] using block

                [^3]: fn 3

                <div style=\"display: none;\">
                   -
                    [^4]: fn 4

                </div>
        "}
        );

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
        let scanned = scan_multiline_footnote(md, &(7..md.len() - 1))?;
        assert_eq!(scanned.end, md.len() - 1);
        assert!(scanned.continuation.is_none());

        let md = "other\n\n[^1]: asdf\n\n    bsdf\n\ncsdf";
        let scanned = scan_multiline_footnote(md, &(7..19))?;
        assert_eq!(&md[19..scanned.end], "    bsdf\n\n");
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
        let scanned = scan_multiline_footnote(md, &(pos - 8 - 11..pos))?;
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
        let (_, range) = pulldown_cmark::Parser::new(md)
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
            scan_list_continuation(md, last_li_start, range.end)?,
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

        do_scan_list_continuation("*\n test", "*\n test")?;
        do_scan_list_continuation("+\n test", "+\n test")?;
        do_scan_list_continuation("1.\n test", "1.\n test")?;
        do_scan_list_continuation("1)\n test", "1)\n test")?;
        do_scan_list_continuation("10.\n test", "10.\n test")?;
        do_scan_list_continuation("10)\n test", "10)\n test")?;
        do_scan_list_continuation("10.\n\n test", "10.\n\n")?;
        do_scan_list_continuation("10.\n\n  test", "10.\n\n")?;
        do_scan_list_continuation("10.\n\n   test", "10.\n\n   test")?;

        let (_, scanned) = scan_list_helper("-\n test")?;
        assert_eq!(scanned.continuation.as_ref().unwrap().unindented_md, "test");
        assert_eq!(
            scanned.continuation.as_ref().unwrap().space_deletion_points,
            vec![(2, 1)]
        );

        let (_, scanned) = scan_list_helper("- test\n\n  -\n\n     test2")?;
        assert_eq!(
            scanned.continuation.as_ref().unwrap().unindented_md,
            "  test2"
        );
        assert_eq!(
            scanned.continuation.as_ref().unwrap().space_deletion_points,
            vec![(13, 3)]
        );

        Ok(())
    }

    #[tokio::test]
    async fn link_reference_upperlowercase() -> Result<()> {
        let (md, _) = read_file("link-reference-upperlowercase.md")?;
        let md = std::str::from_utf8(&md)?;
        let split = md2mdblocks(md).await?;
        assert_eq!(split.blocks.len(), 1);
        assert_eq!(split.blocks[0], "[fOO]\n\n[Foo]: https://github.com/\n");
        Ok(())
    }

    #[tokio::test]
    async fn no_newline_before_eof() -> Result<()> {
        let md = "[link]: https://github.com/\nA paragraph with [link]";
        let split = md2mdblocks(md).await?;
        assert_eq!(split.blocks.len(), 1);
        // Important that there are two \n in front of the [link]: definition
        assert_eq!(
            split.blocks[0],
            "A paragraph with [link]\n\n[link]: https://github.com/\n"
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore] // TODO: Remove when fixed
    async fn completely_ignored_footnotes() -> Result<()> {
        // These footnote are completely definitions are both
        // completely ignored by pulldown_cmark with no trace of them
        // whatsoever in the Event()s.
        // TODO: What to do?
        let md = indoc::indoc! {"
             [^2]: fn2

            - asdf

                [^1]: fn1

            - I'm refing [^1] and [^2]
        "};
        let split = md2mdblocks(md).await?;
        assert_eq!(split.blocks.len(), 1);
        assert_eq!(
            split.blocks[0],
            indoc::indoc! {"
            - asdf

                [^1]: fn1

            - I'm refing [^1] and [^2]

            [^1]: fn1

             [^2]: fn2
            "}
        );
        Ok(())
    }
}
