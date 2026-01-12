use crate::pandata::{Args, Format, FormatOptions};
use polars::io::SerReader;
use polars::prelude::{CsvParseOptions, CsvReadOptions, CsvWriterOptions, IntoLazy, LazyFrame};
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

    fn read_options(&self) -> FormatOptions {
        FormatOptions::from_keys(["separator", "quote-char"])
    }

    fn read(&self, path: &str, args: &Args) -> anyhow::Result<LazyFrame> {
        // TODO: this crashes
        // let lf = LazyCsvReader::new(path)
        //     .with_has_header(true)
        //     .finish()?;

        let mut parse_options = CsvParseOptions::default();
        if let Some(sep) = args.char("separator") {
            parse_options = parse_options.with_separator(sep)
        }
        if let Some(quote_char) = args.char("quote-char") {
            parse_options = parse_options.with_quote_char(Some(quote_char))
        }
        let read_options = CsvReadOptions::default().with_parse_options(parse_options);
        let lf = read_options
            .try_into_reader_with_file_path(Some(PathBuf::from(path)))?
            .finish()?
            .lazy();
        Ok(lf)
    }

    fn write(&self, path: &str, _args: &Args, lf: LazyFrame) -> anyhow::Result<()> {
        let mut options = CsvWriterOptions::default();
        options.maintain_order = true;
        lf.sink_csv(path, options)?;
        Ok(())
    }
}
