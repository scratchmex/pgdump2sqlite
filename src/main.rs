use anyhow::Result;
use clap::Parser;
use pgdump2sqlite::import_from_file;

#[derive(clap::Parser)]
struct Cli {
    pgdump_filename: std::path::PathBuf,
    sqlite_filename: std::path::PathBuf,
}

fn main() -> Result<()> {
    let args = Cli::parse();

    import_from_file(args.pgdump_filename, args.sqlite_filename)?;

    Ok(())
}
