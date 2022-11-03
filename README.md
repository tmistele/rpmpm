Rust rewrite of the server part of [pmpm][pmpm] for fun and profit (?).

Not quite feature-complete but already usable. Just `cargo run` (`--release`) it.

There's an experimental `cmark` feature flag.
If enabled, [`pulldown-cmark`][pulldown-cmark] is used instead of `pandoc` to split the markdown file into blocks.
This should speed things up a lot for larger documents.
You can try it with `cargo run --features cmark` (`--release`) or similar.

[pmpm]: https://github.com/sweichwald/pmpm
[pulldown-cmark]: https://crates.io/crates/pulldown-cmark
