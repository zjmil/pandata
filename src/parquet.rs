use crate::pandata::{Args, Format, FormatOptions};
use polars::prelude::{LazyFrame, ParquetWriteOptions, ScanArgsParquet};
use polars_plan::dsl::{FileWriteFormat, SinkDestination, SinkTarget, UnifiedSinkArgs};

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

    fn read_options(&self) -> FormatOptions {
        FormatOptions::new()
    }

    fn read(&self, path: &str, _args: &Args) -> anyhow::Result<LazyFrame> {
        let args = ScanArgsParquet::default();
        let lf = LazyFrame::scan_parquet(path.into(), args)?;
        Ok(lf)
    }

    fn write(&self, path: &str, _args: &Args, lf: LazyFrame) -> anyhow::Result<()> {
        let options = ParquetWriteOptions::default();
        lf.sink(
            SinkDestination::File {
                target: SinkTarget::Path(path.into()),
            },
            FileWriteFormat::Parquet(options.into()),
            UnifiedSinkArgs::default(),
        )?
        .collect_with_engine(polars::prelude::Engine::Streaming)?;
        Ok(())
    }
}
