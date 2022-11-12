# Rust rewrite of the server part of [pmpm][pmpm]

Not quite feature-complete but already usable. Just `cargo run` (`--release`) it.

There's an experimental `cmark` feature flag.
If enabled, [`pulldown-cmark`][pulldown-cmark] is used instead of `pandoc` to split the markdown file into blocks.
This should speed things up a lot for larger documents.
You can try it with `cargo run --features cmark` (`--release`) or similar.

[pmpm]: https://github.com/sweichwald/pmpm
[pulldown-cmark]: https://crates.io/crates/pulldown-cmark

## How much faster is this?

The `end_to_end` test contains a test case that gives a rough impression (but is not a proper benchmark!).
The test sends a big .md file (~150KiB) to pmpm six times and outputs how long pmpm takes to process it.
Before the third time, a small block in the .md file is modified.
Before the sixth time, a large block in the .md file is modified.

### Original python version

Running

`$ RUST_LOG=end_to_end=debug cargo test --release --test end_to_end pythonbench`

gives

```
Time from pipe send to websocket recv for initial: 1.951543619s
Time from pipe send to websocket recv for hot: 115.613647ms
Time from pipe send to websocket recv for hot: 477.013466ms
Time from pipe send to websocket recv for small block changed: 1.262383539s
Time from pipe send to websocket recv for previous hot: 484.208431ms
Time from pipe send to websocket recv for large block changed: 1.425725076s
```

I used python 3.10.

### Rust version (without `cmark` feature)

Running

`$ RUST_LOG=end_to_end=debug cargo test --release --test end_to_end naivebench`

gives

```
Time from pipe send to websocket recv for initial: 1.526904827s
Time from pipe send to websocket recv for hot: 18.405244ms
Time from pipe send to websocket recv for hot: 15.999644ms
Time from pipe send to websocket recv for small block changed: 731.974341ms
Time from pipe send to websocket recv for previous hot: 13.48669ms
Time from pipe send to websocket recv for large block changed: 852.108441ms
```

I used rust 1.65.

### Rust version (with `cmark` feature)

Running

`$ RUST_LOG=end_to_end=debug cargo test --release --test end_to_end naivebench --features cmark`

gives

```
Time from pipe send to websocket recv for initial: 1.315426906s
Time from pipe send to websocket recv for hot: 1.853992ms
Time from pipe send to websocket recv for hot: 1.549679ms
Time from pipe send to websocket recv for small block changed: 14.749363ms
Time from pipe send to websocket recv for previous hot: 1.982681ms
Time from pipe send to websocket recv for large block changed: 284.143211ms
```

I used rust 1.65.
