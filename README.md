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
