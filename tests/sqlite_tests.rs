#![cfg(feature = "sqlite")]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use pandata::{Args, Format, SqliteFormat};
use polars::prelude::{DataFrame, DataType, IntoLazy, NamedFrom, Series};
use rusqlite::Connection;

static COUNTER: AtomicUsize = AtomicUsize::new(0);
struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "pandata-sqlite-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
    fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn read(path: &Path, args: &Args) -> Result<DataFrame> {
    Ok(SqliteFormat::new()
        .read(path.to_str().unwrap(), args)?
        .collect()?)
}
fn fixture(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path)?;
    connection.execute_batch(include_str!("fixtures/sqlite.sql"))?;
    Ok(connection)
}
#[test]
fn roundtrips_sqlite_types_and_quoted_identifiers() -> Result<()> {
    let temp = TempDir::new()?;
    let input = temp.file("input.db");
    fixture(&input)?;
    let frame = read(&input, &Args::new())?;
    assert_eq!(frame.shape(), (3, 5));
    assert_eq!(frame.column("active")?.dtype(), &DataType::Boolean);
    assert_eq!(frame.column("payload")?.dtype(), &DataType::Binary);
    let output = temp.file("output.sqlite");
    let mut args = Args::new();
    args.insert("sqlite-table", "output \" table".into());
    SqliteFormat::new().write(output.to_str().unwrap(), &args, frame.clone().lazy())?;
    assert!(frame.equals_missing(&read(&output, &args)?));
    assert!(SqliteFormat::new()
        .write(output.to_str().unwrap(), &args, frame.clone().lazy())
        .is_err());
    assert!(frame.equals_missing(&read(&output, &args)?));
    Ok(())
}
#[test]
fn selects_tables_and_rejects_missing_or_invalid_databases() -> Result<()> {
    let temp = TempDir::new()?;
    let input = temp.file("input.sqlite");
    let connection = fixture(&input)?;
    connection.execute_batch("CREATE TABLE other (n INTEGER);")?;
    let error = read(&input, &Args::new()).unwrap_err();
    assert!(error.to_string().contains("--sqlite-table"));
    let mut args = Args::new();
    args.insert("sqlite-table", "other".into());
    assert_eq!(read(&input, &args)?.shape(), (0, 1));
    assert_eq!(read(&input, &args)?.column("n")?.dtype(), &DataType::Int64);
    args.insert("sqlite-table", "missing".into());
    assert!(read(&input, &args).is_err());
    let missing = temp.file("missing.db");
    assert!(read(&missing, &Args::new()).is_err());
    assert!(!missing.exists());
    let invalid = temp.file("invalid.db");
    fs::write(&invalid, "not a database")?;
    assert!(read(&invalid, &Args::new()).is_err());
    let empty = temp.file("empty.db");
    Connection::open(&empty)?;
    assert!(read(&empty, &Args::new()).is_err());
    assert!(SqliteFormat::new()
        .read("/dev/stdin", &Args::new())
        .is_err());
    Ok(())
}
#[test]
fn promotes_numbers_and_rejects_mixed_types() -> Result<()> {
    let temp = TempDir::new()?;
    let input = temp.file("mixed.db");
    let connection = Connection::open(&input)?;
    connection
        .execute_batch("CREATE TABLE data (n); INSERT INTO data VALUES (NULL), (1), (2.5);")?;
    assert_eq!(
        read(&input, &Args::new())?.column("n")?.dtype(),
        &DataType::Float64
    );
    connection.execute("INSERT INTO data VALUES ('text')", [])?;
    assert!(read(&input, &Args::new())
        .unwrap_err()
        .to_string()
        .contains("Mixed SQLite"));
    Ok(())
}
#[test]
fn rolls_back_failed_writes_and_preserves_other_tables() -> Result<()> {
    let temp = TempDir::new()?;
    let output = temp.file("output.db");
    let connection = Connection::open(&output)?;
    connection
        .execute_batch("CREATE TABLE existing (n INTEGER); INSERT INTO existing VALUES (42);")?;
    let frame = DataFrame::new(2, vec![Series::new("n".into(), &[1_u64, u64::MAX]).into()])?;
    assert!(SqliteFormat::new()
        .write(output.to_str().unwrap(), &Args::new(), frame.lazy())
        .is_err());
    assert_eq!(
        connection.query_row("SELECT n FROM existing", [], |row| row.get::<_, i64>(0))?,
        42
    );
    assert_eq!(
        connection.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name = 'data'",
            [],
            |row| row.get::<_, i64>(0)
        )?,
        0
    );
    Ok(())
}
#[cfg(feature = "ndjson")]
#[test]
fn cli_infers_sqlite_extensions_and_selects_tables() -> Result<()> {
    let temp = TempDir::new()?;
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/records.jsonl");
    for extension in ["sqlite", "sqlite3", "db"] {
        let database = temp.file(&format!("records.{extension}"));
        let output = temp.file(&format!("{extension}.jsonl"));
        for (input, output) in [(&fixture, &database), (&database, &output)] {
            let result = std::process::Command::new(env!("CARGO_BIN_EXE_pandata"))
                .arg(input)
                .arg(output)
                .args(["--sqlite-table", "records"])
                .output()?;
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
        let format = pandata::NdjsonFormat::new();
        let expected = format
            .read(fixture.to_str().unwrap(), &Args::new())?
            .collect()?;
        let actual = format
            .read(output.to_str().unwrap(), &Args::new())?
            .collect()?;
        assert!(expected.equals_missing(&actual));
    }
    Ok(())
}

#[test]
fn preserves_empty_and_all_null_schemas() -> Result<()> {
    let temp = TempDir::new()?;
    let input = temp.file("nulls.db");
    let connection = Connection::open(&input)?;
    connection.execute_batch("CREATE TABLE data (n INTEGER, flag BOOLEAN, text TEXT, bytes BLOB, unknown); INSERT INTO data VALUES (NULL, NULL, NULL, NULL, NULL);")?;
    for empty in [false, true] {
        if empty {
            connection.execute("DELETE FROM data", [])?;
        }
        let frame = read(&input, &Args::new())?;
        assert_eq!(
            frame.dtypes(),
            vec![
                DataType::Int64,
                DataType::Boolean,
                DataType::String,
                DataType::Binary,
                DataType::Null
            ]
        );
        let output = temp.file(if empty {
            "empty.sqlite"
        } else {
            "nulls.sqlite"
        });
        SqliteFormat::new().write(output.to_str().unwrap(), &Args::new(), frame.clone().lazy())?;
        assert!(frame.equals_missing(&read(&output, &Args::new())?));
    }
    Ok(())
}

#[test]
fn rejects_invalid_boolean_and_nonfinite_float() -> Result<()> {
    let temp = TempDir::new()?;
    let input = temp.file("boolean.db");
    let connection = Connection::open(&input)?;
    connection.execute_batch("CREATE TABLE data (flag BOOLEAN); INSERT INTO data VALUES (2);")?;
    assert!(read(&input, &Args::new()).is_err());
    connection.execute_batch("DELETE FROM data; INSERT INTO data VALUES ('bad');")?;
    assert!(read(&input, &Args::new()).is_err());
    for value in [f64::NAN, f64::INFINITY] {
        let output = temp.file("float.db");
        let frame = DataFrame::new(1, vec![Series::new("n".into(), &[value]).into()])?;
        assert!(SqliteFormat::new()
            .write(output.to_str().unwrap(), &Args::new(), frame.lazy())
            .is_err());
    }
    Ok(())
}
