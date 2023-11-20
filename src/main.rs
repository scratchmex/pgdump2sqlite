use anyhow::Result;
use clap::Parser;
use pgdump2sqlite::import_from_sql_file;

#[derive(clap::Parser)]
struct Cli {
    pgdump_filename: std::path::PathBuf,
    sqlite_filename: std::path::PathBuf,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    // TODO: don't read the whole file but use an buffered line interator

    import_from_sql_file(args.pgdump_filename, args.sqlite_filename)?;

    Ok(())
}
