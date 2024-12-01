use anyhow::Context;
use anyhow::Result;
use polars::prelude::LazyFrame;
use std::collections::HashMap;

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
    ) -> anyhow::Result<()> {
        let reader = self
            .formats
            .get(from_format)
            .with_context(|| format!("No reader for format: {}", from_format))?;
        let writer = self
            .formats
            .get(to_format)
            .with_context(|| format!("No writer for format: {}", to_format))?;
        let lf = reader.read(from_path)?;
        writer.write(to_path, lf)?;
        Ok(())
    }
}

pub trait Format {
    fn canonical_name(&self) -> &'static str;

    fn read(&self, path: &str) -> Result<LazyFrame>;

    fn write(&self, path: &str, lf: LazyFrame) -> anyhow::Result<()>;
}
