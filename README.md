# pandata

pandata is like pandoc for data. We keep format conversion simple so you can move between common data formats with one command.

## How to install

Use Cargo so anyone can install the CLI locally.

```
cargo install pandata
```

## Usage

Convert files by extension when possible.

```
pandata input.csv output.parquet
```

Use `-` for stdin/stdout, and specify formats when extensions are missing.

```
pandata --from json --to csv - output.csv
pandata input.parquet - --to json
```

## Conversion performance

Local builds use the Rust version pinned in `rust-toolchain.toml`. Both revisions
in a performance comparison must have committed lockfiles compatible with the
comparison compiler; prepare a compatibility update before a dependency upgrade
when the previous lockfile cannot build.

Run all six CSV, JSON (JSON Lines), and Parquet conversions locally:

```sh
POLARS_MAX_THREADS=2 cargo bench --locked --bench conversions
cargo bench --locked --bench conversions -- --cases csv-to-json --output results.json
python3 -m unittest discover -s scripts -p 'test_*.py'
bash scripts/performance-gate.sh BASE_COMMIT HEAD_COMMIT
```

The stable Rust harness generates 100,000 deterministic rows from
`tests/fixtures/benchmark-strings.txt`, warms each conversion, and records 15
samples, their median, and rows per second. It validates every output outside
timed sections. Temporary inputs and outputs are automatically removed.
Measurements include file reads and writes with warmed filesystem caches;
CLI startup and memory usage are excluded. Ordinary `cargo test` does not run
the benchmark.

GitHub and Gitea run the same driver for pull requests and default-branch pushes.
Compare the PR base with its proposed head, or the previous push revision with
the new revision. Build both committed lockfiles using Rust 1.99.1, with the
proposed head's harness and fixture on both revisions, enabling only the three
benchmarked format features. Measure base then head
with two Polars threads. If a median slows by more than 15%, remeasure those
cases in head-then-base order. Fail only regressions exceeding 15% in both
rounds; exactly 15% passes. Build, conversion, validation, missing commit, and
invalid measurement failures fail the gate, including unavailable baselines
(e.g. a branch's first push). Retain raw measurements and build/run logs as CI
artifacts, or in `performance-results/` locally. Run comparisons on an idle
machine; results from separate runners are not comparable.
