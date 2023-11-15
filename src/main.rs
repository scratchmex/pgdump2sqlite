use anyhow::Result;
use clap::Parser;
use std::fs;

#[derive(clap::Parser)]
struct Cli {
    pgdump_filename: std::path::PathBuf,
    sqlite_filename: std::path::PathBuf,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    // TODO: don't read the whole file but use an buffered line interator
    let content = fs::read_to_string(args.pgdump_filename).unwrap();
    let mut import_op = pgdump2sqlite::ImportOp::new(args.sqlite_filename);

    println!("start");

    import_op.parse(content)?;

    Ok(())
}
