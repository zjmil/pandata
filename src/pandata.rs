use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use polars::prelude::LazyFrame;

pub struct Pandata {
    formats: HashMap<String, Box<dyn Format>>,
}

impl Pandata {
    pub fn new() -> Self {
        Pandata {
            formats: HashMap::new(),
        }
    }

    pub fn add_format(&mut self, format: Box<dyn Format>) {
        self.formats
            .insert(format.canonical_name().to_owned(), format);
    }

    pub fn convert(
        &self,
        from_path: &str,
        to_path: &str,
        from_format: &str,
        to_format: &str,
    ) -> Result<()> {
        self.convert_with_options(
            from_path,
            to_path,
            from_format,
            to_format,
            &Args::new(),
            &Args::new(),
        )
    }

    pub fn convert_with_options(
        &self,
        from_path: &str,
        to_path: &str,
        from_format: &str,
        to_format: &str,
        reader_args: &Args,
        writer_args: &Args,
    ) -> Result<()> {
        let reader = self
            .formats
            .get(canonical_format(from_format))
            .with_context(|| format!("No reader for format: {}", from_format))?;
        let writer = self
            .formats
            .get(canonical_format(to_format))
            .with_context(|| format!("No writer for format: {}", to_format))?;
        let _ = reader.read_options().options().count();
        let _ = writer.read_options().options().count();
        let lf = reader
            .read(from_path, reader_args)
            .with_context(|| format!("Cannot read {from_format} input: {from_path}"))?;
        writer
            .write(to_path, writer_args, lf)
            .with_context(|| format!("Cannot write {to_format} output: {to_path}"))?;
        Ok(())
    }
}

pub struct FormatOptions {
    keys: HashSet<String>,
}

impl FormatOptions {
    pub fn new() -> Self {
        let v: [&str; 0] = [];
        Self::from_keys(v)
    }

    pub fn from_keys<'a>(it: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        let keys = it.into_iter().map(|s| s.as_ref().to_owned()).collect();
        Self { keys }
    }

    pub fn options(&self) -> impl Iterator<Item = &String> {
        self.keys.iter()
    }
}

#[derive(Default)]
pub struct Args {
    args: HashMap<String, Vec<String>>,
}

impl Args {
    pub fn new() -> Self {
        Self {
            args: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: &str, value: String) {
        self.args.insert(key.to_owned(), vec![value]);
    }

    pub fn list(&self, key: &str) -> Option<Vec<String>> {
        self.args.get(key).map(|x| x.clone())
    }
    pub fn string(&self, key: &str) -> Option<String> {
        match self.list(key) {
            Some(v) if v.len() >= 1 => v.first().map(|x| x.clone()),
            _ => None,
        }
    }

    pub fn char(&self, key: &str) -> Option<u8> {
        self.string(key)
            .map(|s| s.as_bytes().get(0).map(|x| *x))
            .flatten()
    }
}

pub trait Format {
    fn canonical_name(&self) -> &'static str;

    fn read_options(&self) -> FormatOptions;

    fn read(&self, path: &str, args: &Args) -> Result<LazyFrame>;

    fn write(&self, path: &str, args: &Args, lf: LazyFrame) -> Result<()>;
}

fn canonical_format(format: &str) -> &str {
    match format {
        "arrow" | "feather" => "ipc",
        "jsonl" => "ndjson",
        "sqlite3" | "db" => "sqlite",
        _ => format,
    }
}
