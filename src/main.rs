use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;

use anyhow::Result;
use csv::CsvFormat;
use pandata::{Args, Pandata};
use parquet::ParquetFormat;

mod csv;
mod json;
mod pandata;
mod parquet;

// #[derive(Parser, Debug)]
// #[command(version, about, long_about = None)]
// struct Cli {
//     /// The file to read
//     #[arg(value_name = "FROM_FILE")]
//     from_file: Option<String>,
//
//     /// The file to write
//     #[arg(value_name = "TO_FILE")]
//     to_file: Option<String>,
//
//     /// Force the input format
//     #[arg(short, long, value_name = "FORMAT")]
//     from: Option<String>,
//
//     /// Force the output format
//     #[arg(short, long, value_name = "FORMAT")]
//     to: Option<String>,
// }

#[derive(Default)]
struct Cli {
    from_file: Option<String>,
    to_file: Option<String>,
    from_format: Option<String>,
    to_format: Option<String>,
    from_args: Args,
    to_args: Args,
}

impl Cli {
    fn parse(pandata: &Pandata) -> Cli {
        let mut cli = Cli::default();
        let args: Vec<String> = std::env::args().collect();
        
        let mut idx = 0;
        
        let mut other_args = HashMap::new();
        let mut curr_opt_arg = None
        while idx < args.len() {
            let arg = &args[idx];
            match arg.strip_prefix("--") {
                Some(opt_arg) =>
                    if opt_arg == "from" {
                        
                    }
                None =>
            }
            
        }
        
        cli
    }
}

fn parse_format_path(p: impl AsRef<Path>) -> Option<String> {
    p.as_ref()
        .extension()
        .map(OsStr::to_str)
        .flatten()
        .map(str::to_owned)
}

fn parse_format(format: Option<String>, input_path: &str) -> Option<String> {
    format.or_else(|| parse_format_path(&input_path))
}

fn build_pandata() -> Pandata {
    // build with default formats

    let csv_format = CsvFormat::new();
    let json_format = json::JsonFormat::new();
    let parquet_format = ParquetFormat::new();

    let mut pandata = Pandata::new();
    pandata.add_format(Box::new(csv_format));
    pandata.add_format(Box::new(json_format));
    pandata.add_format(Box::new(parquet_format));

    pandata
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let from_file = match cli.from_file {
        Some(ref x) if x != "-" => x,
        _ => "/dev/stdin",
    };
    let to_file = match cli.to_file {
        Some(ref x) if x != "-" => x,
        _ => "/dev/stdout",
    };

    let from_format = parse_format(cli.from.clone(), &from_file)
        .expect("Unable to parse input format. Must be explicit if reading from stdin.");
    let to_format = parse_format(cli.to.clone(), &to_file)
        .expect("Unable to parse output format. Must be explicit if writing to stdout.");

    println!("pandata: {:?}", cli);

    let pandata = build_pandata();

    pandata.convert(from_file, to_file, &from_format, &to_format)?;

    Ok(())
}
