use crate::pandata::Format;
use polars::prelude::{LazyFrame, ParquetWriteOptions, ScanArgsParquet};

pub struct ParquetFormat;

impl ParquetFormat {
    pub fn new() -> Self {
        ParquetFormat {}
    }
}

impl Format for ParquetFormat {
    fn canonical_name(&self) -> &'static str {
        "parquet"
    }

    fn read(&self, path: &str) -> anyhow::Result<LazyFrame> {
        let args = ScanArgsParquet::default();
        let lf = LazyFrame::scan_parquet(path, args)?;
        Ok(lf)
    }

    fn write(&self, path: &str, lf: LazyFrame) -> anyhow::Result<()> {
        let options = ParquetWriteOptions::default();
        lf.sink_parquet(path, options)?;
        Ok(())
    }
}
