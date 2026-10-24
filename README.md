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

## Supported formats

| Format | Extensions / names | Feature |
| --- | --- | --- |
| CSV | `csv` | `csv` |
| TSV | `tsv` | `tsv` |
| JSON Lines (legacy name) | `json` | `json` |
| NDJSON / JSON Lines | `ndjson`, `jsonl` | `ndjson` |
| Parquet | `parquet` | `parquet` |
| Avro | `avro` | `avro` |
| SQLite | `sqlite`, `sqlite3`, `db` | `sqlite` |
| Arrow IPC / Feather V2 | `ipc`, `arrow`, `feather` | `ipc` |

All formats are enabled by default. Use the same names with `--from` and
`--to` when extensions are missing. The `json` format retains its existing
JSON Lines behavior; JSON arrays and legacy Feather V1 are not supported.
Arrow support reads and writes IPC files, rather than IPC record batch streams.
NDJSON and IPC currently read into memory because Polars 0.44 cannot feed
these lazy scans into streaming sinks. Writes use streaming sinks.

```sh
pandata events.jsonl events.feather
pandata events.arrow events.ndjson
cargo build --no-default-features --features csv,parquet,ipc,ndjson
```

Common data engineering formats include CSV, JSON, Parquet, ORC, and Avro,
as reflected in [Apache Spark’s data sources](https://spark.apache.org/docs/latest/sql-data-sources.html).
CSV, JSON Lines, Parquet, and Avro already have adapters here.
Include `parquet` in custom feature builds: Polars 0.44’s existing cloud/IPC
feature combination fails to compile without it.

Prioritize these additions because [Polars supports lazy IPC and NDJSON I/O](https://docs.pola.rs/api/python/stable/reference/io.html),
and [Feather V2 is the Arrow IPC file format](https://arrow.apache.org/docs/dev/python/feather.html).
For the next phase, evaluate ORC for Hadoop workflows and Excel for spreadsheet
exchange. Define worksheet selection and type handling before adding Excel;
evaluate dependency size and read/write support before adding either format.

## SQLite

Convert a single-table database automatically, or select a table when the database
contains several user tables:

```sh
pandata input.csv output.sqlite
pandata database.db output.parquet --sqlite-table events
pandata input.parquet database.db --sqlite-table events
```

SQLite writes create `data` by default. Use `--sqlite-table` to choose the input
or output table (the same name applies to both for SQLite-to-SQLite conversions).
Existing tables are never overwritten or appended to; choose a new table or file.
Other tables stay intact, and failed inserts roll back the new table and its rows.
SQLite requires file paths; stdin/stdout are unsupported.

Enable the optional `sqlite` feature in custom builds. SQLite is bundled, so no
system SQLite installation is needed. Reads and writes materialize data in memory.
Integers use signed 64-bit storage, floats use 64-bit storage, and text, blobs,
booleans, and nulls are supported. Integer and real values in one input column
promote to floats; other mixed value types produce an error. Empty/all-null
columns use declared types where recognized. Boolean declarations require 0 or 1.
Nested and temporal output types, overflowing unsigned integers, and non-finite
floats produce errors; convert those columns to supported types first.

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
