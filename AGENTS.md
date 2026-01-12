pandata agent guide

Purpose: help agents work consistently in this repo.
Keep changes small, fast, and user-friendly.
Prefer standard Rust tooling and minimal deps.
Target a small binary and fast runtime.
Design for extendable format plugins via features.
Support more formats without extra flags.
If unsure, ask before adding new dependencies.

Build, lint, test
- Build debug: `cargo build`
- Build release: `cargo build --release`
- Run CLI: `cargo run -- <args>`
- Run with release: `cargo run --release -- <args>`
- Format check: `cargo fmt --all -- --check`
- Format fix: `cargo fmt --all`
- Lint all: `cargo clippy --all-targets --all-features -- -D warnings`
- Lint default features: `cargo clippy -- -D warnings`
- Tests: `cargo test`
- Tests with output: `cargo test -- --nocapture`
- Single unit test by name: `cargo test <test_name>`
- Single module test: `cargo test module::tests::<test_name>`
- Single integration test file: `cargo test --test <file_stem>`
- Single test inside integration file: `cargo test --test <file_stem> <test_name>`
- Doc tests only: `cargo test --doc`
- Build without running tests: `cargo test --no-run`
- Benchmarks (if added): `cargo bench`

If commands change, update this file.
No .cursor or Copilot rules found in repo.

Repository shape
- Main binary entry: `src/main.rs`
- Core conversion engine: `src/pandata.rs`
- Format modules live in `src/<format>.rs`
- Formats are registered in `build_pandata()`
- `Format` trait defines read/write contract
- CLI parsing is currently manual and evolving
- Polars lazy APIs are the default data path
- Keep the CLI simple; infer formats when possible

Format/plugin guidance
- Add new formats as separate modules
- Implement `Format` and expose `new()`
- Keep `canonical_name()` lowercase, stable
- Add feature flags for optional formats
- Gate module and registration with `cfg(feature)`
- Avoid heavy dependencies for niche formats
- Prefer streaming/lazy reads and sinks
- Avoid loading full data when a sink exists
- Keep read/write options in `FormatOptions`
- Only expose options that users need
- Favor sensible defaults over extra flags
- Keep format-specific flags namespaced
- If adding plugins, keep interface minimal

Imports and module layout
- Order imports: std, external crates, crate modules
- Group imports with blank lines between groups
- Use explicit imports; avoid glob imports
- Prefer `use crate::...` over relative paths
- Keep module public surface minimal
- Put helper structs near their main use
- Keep type aliases in the module they serve

Formatting
- Follow `rustfmt` defaults; do not fight it
- Use 4-space indentation (default)
- Keep line length reasonable; let rustfmt wrap
- Avoid manual alignment with spaces
- Use trailing commas in multi-line lists
- Prefer `match` for multi-branch control flow
- Avoid deeply nested `if let` chains

Naming
- Types: `PascalCase`
- Functions/vars: `snake_case`
- Constants: `SCREAMING_SNAKE_CASE`
- Features: `kebab-case` in Cargo features
- CLI flags: `kebab-case` with `--`
- Format names: short, lowercase, no dots
- Keep names explicit over clever

Types and ownership
- Prefer owned `String` for CLI inputs
- Accept `&str` or `impl AsRef<Path>` in APIs
- Avoid unnecessary clones; use references
- Use `Option`/`Result` instead of sentinels
- Keep structs small and focused
- Prefer `enum` for known variants
- Use `impl Iterator` for lightweight returns
- Avoid `Box<dyn Trait>` unless dynamic dispatch needed
- If using trait objects, document why

Error handling
- Use `anyhow::Result` for top-level fallible APIs
- Add `Context` when mapping errors
- Avoid `unwrap`/`expect` outside tests
- Prefer early returns with `?`
- Validate inputs close to the boundary
- Include format names/paths in error context
- Keep error messages user-facing and short

Performance
- Prefer `LazyFrame` and streaming sinks
- Avoid collecting to `DataFrame` unless required
- Use `scan_*` APIs over eager reads when possible
- Minimize intermediate allocations
- Avoid logging per-row operations
- Use `--release` for perf testing
- Keep `clippy` clean; avoid `allow` unless justified

CLI behavior
- Keep default behavior simple and predictable
- Infer format from file extension when possible
- Require explicit format only for stdin/stdout
- Prefer consistent `--from/--to` flag names
- Keep help text short and action-oriented
- Avoid breaking changes without discussion

Testing guidance
- Prefer integration tests for format conversions
- Use small fixtures; avoid large binaries
- Keep tests deterministic and fast
- Test error paths for invalid formats
- Avoid testing Rust stdlib behavior
- If adding a test, add a fixture in `tests/`
- Use `insta`-style snapshots only if necessary

Docs and examples
- Update README only when behavior changes
- Use imperative mood in docs
- Explain why a flag exists, not just what
- Keep examples minimal and copy-pastable

Git and workflow
- Do not commit or push unless asked
- Create new branches for features
- Keep commits focused with short messages
- Avoid editing generated files in `target/`

Notes
- `Cargo.lock` is committed; keep it updated
- Prefer stable Rust unless required
- When in doubt, ask for clarification
- Keep this file updated as rules evolve

End of agent guide.
