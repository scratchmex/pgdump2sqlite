use std::fs;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use pgdump2sqlite::import_from_file;

#[derive(clap::Parser)]
struct Cli {
    pgdump_filename: std::path::PathBuf,
    sqlite_filename: std::path::PathBuf,
    #[arg(
        short,
        help = "delete the dst sqlite file if exists",
        default_value_t = false
    )]
    force: bool,
}

fn main() -> Result<()> {
    let args = Cli::parse();

    if args.sqlite_filename.exists() {
        if !args.force {
            return Err(anyhow!("{:?} exists, delete first", args.sqlite_filename));
        }

        fs::remove_file(&args.sqlite_filename).context("deleting existing sqlite file")?
    }

    import_from_file(args.pgdump_filename, args.sqlite_filename)?;

    Ok(())
}
