use crate::pandata::Format;
use polars::io::SerReader;
use polars::prelude::{CsvReadOptions, CsvWriterOptions, IntoLazy, LazyFrame};
use std::path::PathBuf;

pub struct CsvFormat;

impl CsvFormat {
    pub fn new() -> Self {
        CsvFormat {}
    }
}

impl Format for CsvFormat {
    fn canonical_name(&self) -> &'static str {
        "csv"
    }

    fn read(&self, path: &str) -> anyhow::Result<LazyFrame> {
        // TODO: this crashes
        // let lf = LazyCsvReader::new(path)
        //     .with_has_header(true)
        //     .finish()?;
        let lf = CsvReadOptions::default()
            .try_into_reader_with_file_path(Some(PathBuf::from(path)))?
            .finish()?
            .lazy();
        Ok(lf)
    }

    fn write(&self, path: &str, lf: LazyFrame) -> anyhow::Result<()> {
        let options = CsvWriterOptions::default();
        lf.sink_csv(path, options)?;
        Ok(())
    }
}
