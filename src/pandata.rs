use anyhow::Context;
use anyhow::Result;
use polars::prelude::LazyFrame;
use std::collections::{HashMap, HashSet};
use std::iter::empty;

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
        let reader_args = Args::new();
        let reader = self
            .formats
            .get(from_format)
            .with_context(|| format!("No reader for format: {}", from_format))?;
        let writer_args = Args::new();
        let writer = self
            .formats
            .get(to_format)
            .with_context(|| format!("No writer for format: {}", to_format))?;
        let lf = reader.read(from_path, &reader_args)?;
        writer.write(to_path, &writer_args, lf)?;
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

    pub fn list(&self, key: &str) -> Option<Vec<String>> {
        self.args.get(key).map(|x| x.clone())
    }
    pub fn string(&self, key: &str) -> Option<String> {
        match self.list(key) {
            Some(v) if v.len() >= 1 => v.first().map(|x| x.clone()),
            _ => None,
        }
    }

    pub fn long(&self, key: &str) -> Option<i64> {
        self.string(key).map(|s| s.parse().ok()).flatten()
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
