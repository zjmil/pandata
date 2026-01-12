use anyhow::Result;
use pandata::build_pandata;
use pandata::parse_format;

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
}

impl Cli {
    fn parse() -> Cli {
        let mut cli = Cli::default();
        let args: Vec<String> = std::env::args().skip(1).collect();
        let mut idx = 0;

        while idx < args.len() {
            let arg = &args[idx];
            match arg.as_str() {
                "--from" => {
                    idx += 1;
                    if let Some(value) = args.get(idx) {
                        cli.from_format = Some(value.to_owned());
                    }
                }
                "--to" => {
                    idx += 1;
                    if let Some(value) = args.get(idx) {
                        cli.to_format = Some(value.to_owned());
                    }
                }
                _ => {
                    if cli.from_file.is_none() {
                        cli.from_file = Some(arg.to_owned());
                    } else if cli.to_file.is_none() {
                        cli.to_file = Some(arg.to_owned());
                    }
                }
            }
            idx += 1;
        }

        cli
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let from_file = match cli.from_file.as_deref() {
        Some(x) if x != "-" => x,
        _ => "/dev/stdin",
    };
    let to_file = match cli.to_file.as_deref() {
        Some(x) if x != "-" => x,
        _ => "/dev/stdout",
    };

    let from_format = parse_format(cli.from_format.clone(), from_file)
        .expect("Unable to parse input format. Must be explicit if reading from stdin.");
    let to_format = parse_format(cli.to_format.clone(), to_file)
        .expect("Unable to parse output format. Must be explicit if writing to stdout.");

    let pandata = build_pandata();

    pandata.convert(from_file, to_file, &from_format, &to_format)?;

    Ok(())
}
