use anyhow::{bail, ensure, Context, Result};
use polars::prelude::{AnyValue, Column, DataFrame, DataType, IntoLazy, LazyFrame, Series};
use rusqlite::types::{Value, ValueRef};
use rusqlite::{params_from_iter, Connection, OpenFlags};

use crate::pandata::{Args, Format, FormatOptions};

#[derive(Default)]
pub struct SqliteFormat;

impl SqliteFormat {
    pub fn new() -> Self {
        Self
    }
}

fn quote_identifier(name: &str) -> Result<String> {
    ensure!(
        !name.is_empty() && !name.contains('\0'),
        "Invalid SQLite identifier"
    );
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

fn validate_path(path: &str) -> Result<()> {
    ensure!(
        !matches!(
            path,
            "-" | "/dev/stdin" | "/dev/stdout" | "/dev/stderr" | ":memory:"
        ),
        "SQLite requires a database file path"
    );
    Ok(())
}

fn declared_dtype(declaration: &str) -> DataType {
    let declaration = declaration.to_ascii_uppercase();
    if declaration == "BOOLEAN" || declaration == "BOOL" {
        DataType::Boolean
    } else if declaration.contains("INT") {
        DataType::Int64
    } else if declaration.contains("CHAR")
        || declaration.contains("CLOB")
        || declaration.contains("TEXT")
    {
        DataType::String
    } else if declaration.contains("REAL")
        || declaration.contains("FLOA")
        || declaration.contains("DOUB")
    {
        DataType::Float64
    } else if declaration.contains("BLOB") {
        DataType::Binary
    } else {
        DataType::Null
    }
}

impl Format for SqliteFormat {
    fn canonical_name(&self) -> &'static str {
        "sqlite"
    }

    fn read_options(&self) -> FormatOptions {
        FormatOptions::from_keys(["sqlite-table"])
    }

    fn read(&self, path: &str, args: &Args) -> Result<LazyFrame> {
        validate_path(path)?;
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("Cannot open SQLite database: {path}"))?;
        let mut tables_statement = connection.prepare(
            r"SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\' ORDER BY name",
        )?;
        let tables = tables_statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let table = match args.string("sqlite-table") {
            Some(table) => {
                ensure!(
                    tables.contains(&table),
                    "No SQLite table '{table}' in {path}"
                );
                table
            }
            None => {
                ensure!(
                    !tables.is_empty(),
                    "No user tables in SQLite database: {path}"
                );
                ensure!(
                    tables.len() == 1,
                    "SQLite database has multiple tables; select one with --sqlite-table: {}",
                    tables.join(", ")
                );
                tables[0].clone()
            }
        };
        let mut statement =
            connection.prepare(&format!("SELECT * FROM {}", quote_identifier(&table)?))?;
        let metadata: Vec<_> = statement
            .columns()
            .iter()
            .map(|column| {
                (
                    column.name().to_owned(),
                    declared_dtype(column.decl_type().unwrap_or("")),
                )
            })
            .collect();
        let mut values: Vec<Vec<AnyValue<'static>>> = vec![Vec::new(); metadata.len()];
        let mut inferred = vec![DataType::Null; metadata.len()];
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            for (index, column) in values.iter_mut().enumerate() {
                let value = match row.get_ref(index)? {
                    ValueRef::Null => AnyValue::Null,
                    ValueRef::Integer(value) if metadata[index].1 == DataType::Boolean => {
                        ensure!(
                            value == 0 || value == 1,
                            "Invalid boolean in SQLite column '{}'",
                            metadata[index].0
                        );
                        AnyValue::Boolean(value != 0)
                    }
                    ValueRef::Integer(value) => AnyValue::Int64(value),
                    ValueRef::Real(value) => AnyValue::Float64(value),
                    ValueRef::Text(value) => {
                        AnyValue::StringOwned(std::str::from_utf8(value)?.into())
                    }
                    ValueRef::Blob(value) => AnyValue::BinaryOwned(value.to_vec()),
                };
                ensure!(
                    metadata[index].1 != DataType::Boolean
                        || matches!(value, AnyValue::Null | AnyValue::Boolean(_)),
                    "Invalid boolean in SQLite column '{}'",
                    metadata[index].0
                );
                let dtype = value.dtype();
                if dtype != DataType::Null {
                    inferred[index] = match (&inferred[index], &dtype) {
                        (DataType::Null, _) => dtype,
                        (left, right) if left == right => dtype,
                        (DataType::Int64, DataType::Float64)
                        | (DataType::Float64, DataType::Int64) => DataType::Float64,
                        _ => bail!("Mixed SQLite value types in column '{}'", metadata[index].0),
                    };
                }
                column.push(value);
            }
        }
        let columns = metadata
            .into_iter()
            .zip(values)
            .zip(inferred)
            .map(|(((name, declared), mut values), inferred)| {
                let dtype = if inferred == DataType::Null {
                    declared
                } else {
                    inferred
                };
                if dtype == DataType::Float64 {
                    for value in &mut values {
                        if let AnyValue::Int64(integer) = value {
                            *value = AnyValue::Float64(*integer as f64);
                        }
                    }
                }
                Ok(Column::from(Series::from_any_values_and_dtype(
                    name.into(),
                    &values,
                    &dtype,
                    true,
                )?))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(DataFrame::new(columns)?.lazy())
    }

    fn write(&self, path: &str, args: &Args, lf: LazyFrame) -> Result<()> {
        validate_path(path)?;
        let frame = lf
            .collect()
            .context("Cannot collect data for SQLite output")?;
        ensure!(
            frame.width() > 0,
            "SQLite output requires at least one column"
        );
        let table = quote_identifier(
            &args
                .string("sqlite-table")
                .unwrap_or_else(|| "data".to_owned()),
        )?;
        let definitions = frame
            .get_columns()
            .iter()
            .map(|column| {
                let sql_type = match column.dtype() {
                    DataType::Boolean => "BOOLEAN",
                    DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64 => "INTEGER",
                    DataType::Float32 | DataType::Float64 => "REAL",
                    DataType::String => "TEXT",
                    DataType::Binary => "BLOB",
                    DataType::Null => "",
                    dtype => bail!(
                        "Unsupported SQLite output type {dtype} in column '{}'",
                        column.name()
                    ),
                };
                Ok(format!("{} {sql_type}", quote_identifier(column.name())?))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut connection =
            Connection::open(path).with_context(|| format!("Cannot open SQLite output: {path}"))?;
        let transaction = connection.transaction()?;
        transaction.execute(&format!("CREATE TABLE {table} ({})", definitions.join(", ")), [])
            .with_context(|| format!("Cannot create SQLite table {table} in {path}; choose a new table or output file"))?;
        {
            let placeholders = vec!["?"; frame.width()].join(", ");
            let mut insert =
                transaction.prepare(&format!("INSERT INTO {table} VALUES ({placeholders})"))?;
            let mut values = Vec::with_capacity(frame.width());
            for row in 0..frame.height() {
                values.clear();
                for column in frame.get_columns() {
                    values.push(sqlite_value(column.get(row)?)?);
                }
                insert.execute(params_from_iter(&values))?;
            }
        }
        transaction
            .commit()
            .with_context(|| format!("Cannot commit SQLite output: {path}"))?;
        Ok(())
    }
}

fn sqlite_value(value: AnyValue<'_>) -> Result<Value> {
    Ok(match value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(value) => Value::Integer(i64::from(value)),
        AnyValue::Int8(value) => Value::Integer(value.into()),
        AnyValue::Int16(value) => Value::Integer(value.into()),
        AnyValue::Int32(value) => Value::Integer(value.into()),
        AnyValue::Int64(value) => Value::Integer(value),
        AnyValue::UInt8(value) => Value::Integer(value.into()),
        AnyValue::UInt16(value) => Value::Integer(value.into()),
        AnyValue::UInt32(value) => Value::Integer(value.into()),
        AnyValue::UInt64(value) => Value::Integer(
            value
                .try_into()
                .context("SQLite integer exceeds signed 64-bit range")?,
        ),
        AnyValue::Float32(value) => {
            ensure!(
                value.is_finite(),
                "SQLite output cannot preserve non-finite floats"
            );
            Value::Real(value.into())
        }
        AnyValue::Float64(value) => {
            ensure!(
                value.is_finite(),
                "SQLite output cannot preserve non-finite floats"
            );
            Value::Real(value)
        }
        AnyValue::String(value) => Value::Text(value.to_owned()),
        AnyValue::StringOwned(value) => Value::Text(value.to_string()),
        AnyValue::Binary(value) => Value::Blob(value.to_vec()),
        AnyValue::BinaryOwned(value) => Value::Blob(value),
        value => bail!("Unsupported SQLite value: {value}"),
    })
}
