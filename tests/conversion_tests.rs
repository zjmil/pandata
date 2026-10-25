use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use pandata::{build_pandata, Args, Format};
use polars::prelude::{Column, DataFrame, DataType, IntoLazy, NamedFrom, Series};

#[cfg(feature = "avro")]
use pandata::AvroFormat;
#[cfg(feature = "csv")]
use pandata::CsvFormat;
#[cfg(feature = "json")]
use pandata::JsonFormat;
#[cfg(feature = "parquet")]
use pandata::ParquetFormat;
#[cfg(feature = "tsv")]
use pandata::TsvFormat;

#[cfg(feature = "ipc")]
use pandata::IpcFormat;
#[cfg(feature = "ndjson")]
use pandata::NdjsonFormat;

#[cfg(feature = "sqlite")]
use pandata::SqliteFormat;

#[derive(Clone, Copy)]
enum FormatKind {
    #[cfg(feature = "sqlite")]
    Sqlite,
    #[cfg(feature = "ipc")]
    Ipc,
    #[cfg(feature = "ndjson")]
    Ndjson,
    #[cfg(feature = "csv")]
    Csv,
    #[cfg(feature = "json")]
    Json,
    #[cfg(feature = "parquet")]
    Parquet,
    #[cfg(feature = "tsv")]
    Tsv,
    #[cfg(feature = "avro")]
    Avro,
}

impl FormatKind {
    fn name(self) -> &'static str {
        match self {
            #[cfg(feature = "sqlite")]
            FormatKind::Sqlite => "sqlite",
            #[cfg(feature = "ipc")]
            FormatKind::Ipc => "ipc",
            #[cfg(feature = "ndjson")]
            FormatKind::Ndjson => "ndjson",
            #[cfg(feature = "csv")]
            FormatKind::Csv => "csv",
            #[cfg(feature = "json")]
            FormatKind::Json => "json",
            #[cfg(feature = "parquet")]
            FormatKind::Parquet => "parquet",
            #[cfg(feature = "tsv")]
            FormatKind::Tsv => "tsv",
            #[cfg(feature = "avro")]
            FormatKind::Avro => "avro",
        }
    }

    fn extension(self) -> &'static str {
        self.name()
    }
}

static TEMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Result<Self> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut path = std::env::temp_dir();
        path.push(format!(
            "pandata-test-{}-{}-{}",
            std::process::id(),
            nanos,
            counter
        ));
        fs::create_dir(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn sample_dataframe() -> Result<DataFrame> {
    let int_col = Series::new("int_col".into(), &[Some(1_i64), None, Some(-7), Some(42)]);
    let float_col = Series::new(
        "float_col".into(),
        &[Some(1.25_f64), Some(-2.5), None, Some(3.0)],
    );
    let bool_col = Series::new(
        "bool_col".into(),
        &[Some(true), Some(false), None, Some(true)],
    );
    let string_col = Series::new(
        "string_col".into(),
        &[
            Some("plain"),
            Some("comma, value"),
            Some("quote \"here\""),
            Some("utf8 café"),
        ],
    );
    let date_str = Series::new(
        "date_str".into(),
        &[
            Some("2024-01-02"),
            Some("1999-12-31"),
            None,
            Some("2020-02-29"),
        ],
    );
    let timestamp_str = Series::new(
        "timestamp_str".into(),
        &[
            Some("2024-01-02T03:04:05Z"),
            None,
            Some("1999-12-31T23:59:59Z"),
            Some("2020-02-29T12:00:00Z"),
        ],
    );

    Ok(DataFrame::new(
        4,
        vec![
            Column::from(int_col),
            Column::from(float_col),
            Column::from(bool_col),
            Column::from(string_col),
            Column::from(date_str),
            Column::from(timestamp_str),
        ],
    )?)
}

fn format_for(kind: FormatKind) -> Box<dyn Format> {
    match kind {
        #[cfg(feature = "sqlite")]
        FormatKind::Sqlite => Box::new(SqliteFormat::new()),
        #[cfg(feature = "ipc")]
        FormatKind::Ipc => Box::new(IpcFormat::new()),
        #[cfg(feature = "ndjson")]
        FormatKind::Ndjson => Box::new(NdjsonFormat::new()),
        #[cfg(feature = "csv")]
        FormatKind::Csv => Box::new(CsvFormat::new()),
        #[cfg(feature = "json")]
        FormatKind::Json => Box::new(JsonFormat::new()),
        #[cfg(feature = "parquet")]
        FormatKind::Parquet => Box::new(ParquetFormat::new()),
        #[cfg(feature = "tsv")]
        FormatKind::Tsv => Box::new(TsvFormat::new()),
        #[cfg(feature = "avro")]
        FormatKind::Avro => Box::new(AvroFormat::new()),
    }
}

fn read_frame(kind: FormatKind, path: &Path) -> Result<DataFrame> {
    let format = format_for(kind);
    let lf = format.read(path.to_str().unwrap(), &Args::new())?;
    Ok(lf.collect()?)
}

fn write_frame(kind: FormatKind, path: &Path, df: &DataFrame) -> Result<()> {
    let format = format_for(kind);
    format.write(path.to_str().unwrap(), &Args::new(), df.clone().lazy())?;
    Ok(())
}

fn assert_frames_equal(expected: &DataFrame, actual: &DataFrame) -> Result<()> {
    assert_eq!(expected.shape(), actual.shape());
    assert_eq!(expected.get_column_names(), actual.get_column_names());

    for (expected_col, actual_col) in expected.columns().iter().zip(actual.columns()) {
        assert_eq!(expected_col.name(), actual_col.name());

        let expected_dtype = expected_col.dtype();
        let actual_dtype = actual_col.dtype();

        if expected_dtype == actual_dtype {
            assert!(
                expected_col.equals_missing(actual_col),
                "column {} differs: expected {:?} actual {:?}",
                expected_col.name(),
                expected_col.as_materialized_series(),
                actual_col.as_materialized_series()
            );
            continue;
        }

        if expected_dtype.is_numeric() && actual_dtype.is_numeric() {
            let expected_series = expected_col
                .as_materialized_series()
                .cast(&DataType::Float64)?;
            let actual_series = actual_col
                .as_materialized_series()
                .cast(&DataType::Float64)?;
            assert!(
                expected_series.equals_missing(&actual_series),
                "column {} differs",
                expected_col.name()
            );
            continue;
        }

        assert_eq!(expected_dtype, actual_dtype);
    }

    Ok(())
}

fn assert_conversion(from: FormatKind, to: FormatKind) -> Result<()> {
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join(format!("input.{}", from.extension()));
    let output_path = temp_dir.path().join(format!("output.{}", to.extension()));

    let df = sample_dataframe()?;
    write_frame(from, &input_path, &df)?;

    let expected = read_frame(from, &input_path)?;

    let pandata = build_pandata();
    pandata.convert(
        input_path.to_str().unwrap(),
        output_path.to_str().unwrap(),
        from.name(),
        to.name(),
    )?;

    let actual = read_frame(to, &output_path)?;
    assert_frames_equal(&expected, &actual)?;
    Ok(())
}

#[cfg(all(feature = "csv", feature = "json"))]
#[test]
fn converts_csv_to_json() -> Result<()> {
    assert_conversion(FormatKind::Csv, FormatKind::Json)
}

#[cfg(all(feature = "csv", feature = "parquet"))]
#[test]
fn converts_csv_to_parquet() -> Result<()> {
    assert_conversion(FormatKind::Csv, FormatKind::Parquet)
}

#[cfg(all(feature = "json", feature = "csv"))]
#[test]
fn converts_json_to_csv() -> Result<()> {
    assert_conversion(FormatKind::Json, FormatKind::Csv)
}

#[cfg(all(feature = "json", feature = "parquet"))]
#[test]
fn converts_json_to_parquet() -> Result<()> {
    assert_conversion(FormatKind::Json, FormatKind::Parquet)
}

#[cfg(all(feature = "parquet", feature = "csv"))]
#[test]
fn converts_parquet_to_csv() -> Result<()> {
    assert_conversion(FormatKind::Parquet, FormatKind::Csv)
}

#[cfg(all(feature = "parquet", feature = "json"))]
#[test]
fn converts_parquet_to_json() -> Result<()> {
    assert_conversion(FormatKind::Parquet, FormatKind::Json)
}

#[cfg(all(feature = "csv", feature = "tsv"))]
#[test]
fn converts_csv_to_tsv() -> Result<()> {
    assert_conversion(FormatKind::Csv, FormatKind::Tsv)
}

#[cfg(all(feature = "tsv", feature = "csv"))]
#[test]
fn converts_tsv_to_csv() -> Result<()> {
    assert_conversion(FormatKind::Tsv, FormatKind::Csv)
}

#[cfg(all(feature = "avro", feature = "csv"))]
#[test]
fn converts_csv_to_avro() -> Result<()> {
    assert_conversion(FormatKind::Csv, FormatKind::Avro)
}

#[cfg(all(feature = "avro", feature = "json"))]
#[test]
fn converts_avro_to_json() -> Result<()> {
    assert_conversion(FormatKind::Avro, FormatKind::Json)
}

#[cfg(all(feature = "ipc", feature = "csv"))]
#[test]
fn converts_csv_to_ipc_and_back() -> Result<()> {
    assert_conversion(FormatKind::Csv, FormatKind::Ipc)?;
    assert_conversion(FormatKind::Ipc, FormatKind::Csv)
}

#[cfg(all(feature = "ndjson", feature = "parquet"))]
#[test]
fn converts_ndjson_to_parquet_and_back() -> Result<()> {
    assert_conversion(FormatKind::Ndjson, FormatKind::Parquet)?;
    assert_conversion(FormatKind::Parquet, FormatKind::Ndjson)
}

#[cfg(all(feature = "ipc", feature = "ndjson"))]
#[test]
fn cli_infers_new_format_extensions() -> Result<()> {
    let temp = TempDir::new()?;
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/records.jsonl");
    let expected = read_frame(FormatKind::Ndjson, &fixture)?;
    for extension in ["ipc", "arrow", "feather"] {
        let binary = temp.path().join(format!("records.{extension}"));
        let output = temp.path().join(format!("{extension}.ndjson"));
        for (input, output) in [(&fixture, &binary), (&binary, &output)] {
            let result = std::process::Command::new(env!("CARGO_BIN_EXE_pandata"))
                .arg(input)
                .arg(output)
                .output()?;
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
        assert_frames_equal(&expected, &read_frame(FormatKind::Ndjson, &output)?)?;
    }
    Ok(())
}

#[cfg(all(feature = "json", feature = "ndjson"))]
#[test]
fn reads_json_array_matching_json_lines() -> Result<()> {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let expected = read_frame(FormatKind::Ndjson, &fixtures.join("records.jsonl"))?;
    let actual = read_frame(FormatKind::Json, &fixtures.join("records.json"))?;
    assert_frames_equal(&expected, &actual)
}

#[cfg(feature = "json")]
#[test]
fn writes_json_array() -> Result<()> {
    let temp = TempDir::new()?;
    let output = temp.path().join("output.json");
    write_frame(FormatKind::Json, &output, &sample_dataframe()?)?;
    let text = fs::read_to_string(&output)?;
    assert!(text.trim_start().starts_with('['), "{text}");
    assert!(text.trim_end().ends_with(']'), "{text}");
    Ok(())
}

#[cfg(feature = "json")]
#[test]
fn rejects_json_lines_as_json() -> Result<()> {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/records.jsonl");
    assert!(JsonFormat::new()
        .read(fixture.to_str().unwrap(), &Args::new())
        .and_then(|lf| Ok(lf.collect()?))
        .is_err());
    Ok(())
}

#[cfg(feature = "ipc")]
#[test]
fn rejects_invalid_ipc() -> Result<()> {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/records.jsonl");
    assert!(IpcFormat::new()
        .read(fixture.to_str().unwrap(), &Args::new())
        .and_then(|lf| Ok(lf.collect()?))
        .is_err());
    Ok(())
}

#[test]
fn rejects_unknown_format() {
    let error = build_pandata()
        .convert("unused", "unused", "unknown", "unknown")
        .unwrap_err();
    assert!(error.to_string().contains("No reader for format: unknown"));
}

#[cfg(all(feature = "sqlite", feature = "csv"))]
#[test]
fn converts_csv_to_sqlite_and_back() -> Result<()> {
    assert_conversion(FormatKind::Csv, FormatKind::Sqlite)?;
    assert_conversion(FormatKind::Sqlite, FormatKind::Csv)
}

#[cfg(all(feature = "sqlite", feature = "parquet"))]
#[test]
fn converts_parquet_to_sqlite_and_back() -> Result<()> {
    assert_conversion(FormatKind::Parquet, FormatKind::Sqlite)?;
    assert_conversion(FormatKind::Sqlite, FormatKind::Parquet)
}
