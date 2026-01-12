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

#[derive(Clone, Copy)]
enum FormatKind {
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

    Ok(DataFrame::new(vec![
        Column::from(int_col),
        Column::from(float_col),
        Column::from(bool_col),
        Column::from(string_col),
        Column::from(date_str),
        Column::from(timestamp_str),
    ])?)
}

fn format_for(kind: FormatKind) -> Box<dyn Format> {
    match kind {
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

    for (expected_col, actual_col) in expected.get_columns().iter().zip(actual.get_columns()) {
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
