mod parser;

use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    vec,
};

use anyhow::{ensure, Context, Ok, Result};
use itertools::Itertools;
use parser::Statement;
use rusqlite;

enum DumpContext<P: AsRef<std::path::Path>> {
    Plain { sql_filepath: P },
    Tar { tar_filepath: P },
}

fn get_file_as_string_from_tar<P: AsRef<std::path::Path>>(
    tar_file: P,
    filename: &'_ str,
) -> Result<String> {
    let mut archive = tar::Archive::new(File::open(tar_file)?);
    let mut restore_sql = archive
        .entries()
        .context("getting tar entries")?
        .filter_map(|e| e.ok())
        .filter(|x| x.path().unwrap().to_str().unwrap() == filename)
        .next()
        .expect(&format!("{} file in the tar", filename));

    let mut input = String::new();
    restore_sql
        .read_to_string(&mut input)
        .context(format!("reading {} file as string", filename))?;

    Ok(input)
}

impl parser::ColumnType {
    fn to_sqlite_type(&self) -> &'_ str {
        match self {
            parser::ColumnType::Integer => "integer",
            parser::ColumnType::Text => "text",
            parser::ColumnType::Boolean => "integer",
            parser::ColumnType::Real => "real",
            parser::ColumnType::Unknown => "",
        }
    }
}

fn stmt_to_sql(stmt: &Statement) -> Result<String> {
    let sql_stmt = match stmt {
        Statement::CreateTable(ct) => {
            format!(
                "create table [{}] ( {} )",
                ct.name,
                ct.columns
                    .iter()
                    .map(|x| (&x.name, x.dtype.to_sqlite_type()))
                    .map(|(s, d)| format!("[{s}] {d}"))
                    .join(", ")
            )
        }
        Statement::Copy(ct) => {
            format!(
                "insert into [{}] ( {} ) values ( {} )",
                ct.name,
                ct.columns.iter().map(|c| format!("[{c}]")).join(", "),
                std::iter::repeat("?").take(ct.columns.len()).join(", ")
            )
        }
    };

    Ok(sql_stmt)
}

fn create_tables_in_sqlite<P: AsRef<std::path::Path>>(
    stmts: Vec<&Statement>,
    sqlite_path: P,
) -> Result<()> {
    ensure!(stmts.iter().all(|x| matches!(x, Statement::CreateTable(_))));

    let sql_stmts = stmts
        .iter()
        .map(|x| stmt_to_sql(x))
        .collect::<Result<Vec<_>>>()?;

    let mut conn = rusqlite::Connection::open(sqlite_path)?;
    let txn = conn.transaction()?;

    for ref sql_stmt in sql_stmts {
        println!("{sql_stmt}");
        txn.execute(sql_stmt, ())?;
    }

    txn.commit()?;

    Ok(())
}

fn get_rows_for_copy<P: AsRef<std::path::Path>>(
    stmt: &Statement,
    dump_context: &DumpContext<P>,
) -> Result<Vec<Vec<Option<String>>>> {
    let Statement::Copy(stmt) = stmt else {
        anyhow::bail!("not a copy stmt");
    };

    let rows = match dump_context {
        DumpContext::Plain { sql_filepath } => {
            ensure!(
                stmt.from.to_lowercase() == "stdin",
                "copy from not stdin not supported for plain dump"
            );

            let f = File::open(sql_filepath).context("opening sql filepath")?;
            let mut reader = BufReader::new(f);

            reader.seek_relative(stmt.end as i64)?;

            // TODO: a buffer that just drops the values, I don't want them
            let mut buf = vec![];
            reader
                .read_until(b'\n', &mut buf)
                .context("skipping until EOL, when data starts")?;
            drop(buf);

            // TODO: failable stream of lines
            // -> impl IntoIterator<Item = Vec<&'a str>>
            reader
                .lines()
                .map(|x| x.unwrap())
                .take_while(|x| x != r"\.")
                .map(|l| {
                    l.split('\t')
                        .map(|v| match v {
                            r"\N" => None,
                            _ => Some(v.to_owned()),
                        })
                        .collect_vec()
                })
                .collect_vec()
        }
        DumpContext::Tar { tar_filepath } => {
            if stmt.from.to_lowercase() == "stdin" {
                println!("warning: we don't support copy from stdin for tar dump and we have one, skipping it");

                vec![]
            } else {
                let data = get_file_as_string_from_tar(
                    tar_filepath,
                    stmt.from.strip_prefix("$$PATH$$/").expect(&format!(
                        "{} from path should have the $$PATH$$ prefix",
                        stmt.from
                    )),
                )?;

                data.lines()
                    .take_while(|&x| x != r"\.")
                    .map(|l| {
                        l.split('\t')
                            .map(|v| match v {
                                r"\N" => None,
                                _ => Some(v.to_owned()),
                            })
                            .collect_vec()
                    })
                    .collect_vec()
            }
        }
    };

    // TODO: cast the two iterators ^^ to a common one so we can do this without repeating code
    // let rows = lines
    //     .take_while(|x| x != r"\.")
    //     .map(|l| {
    //         l.split('\t')
    //             .map(|v| match v {
    //                 r"\N" => None,
    //                 _ => Some(v.to_owned()),
    //             })
    //             .collect_vec()
    //     })
    //     .collect_vec();

    Ok(rows)
}
fn insert_data_in_sqlite<P: AsRef<std::path::Path>>(
    stmts: Vec<&Statement>,
    dump_context: &DumpContext<P>,
    sqlite_path: P,
) -> Result<()> {
    ensure!(stmts.iter().all(|x| matches!(x, Statement::Copy(_))));

    let mut conn = rusqlite::Connection::open(sqlite_path)?;

    for stmt in stmts {
        let sql_stmt = stmt_to_sql(stmt)?;
        println!("{sql_stmt}");
        let mut rows_affected = 0;

        let txn = conn.transaction()?;
        {
            let mut prepared_insert = txn.prepare(&sql_stmt)?;
            for row in get_rows_for_copy(stmt, &dump_context)? {
                rows_affected += prepared_insert.execute(rusqlite::params_from_iter(row))?;
            }
        }
        txn.commit()?;
        println!("^^ rows affected: {rows_affected}");
    }

    Ok(())
}

pub fn import_from_file<P: AsRef<std::path::Path>>(file: P, sqlite_path: P) -> Result<()> {
    let extension = file
        .as_ref()
        .extension()
        .expect("cannot get extension of file")
        .to_str()
        .expect("invalid extension?");

    let dump_context = match extension {
        "tar" => DumpContext::Tar { tar_filepath: file },
        "sql" => DumpContext::Plain { sql_filepath: file },
        _ => anyhow::bail!("unsupported file extension: {}", extension),
    };

    let input = match &dump_context {
        DumpContext::Plain { sql_filepath: file } => {
            std::fs::read_to_string(&file).context("reading sql file")?
        }
        DumpContext::Tar { tar_filepath: file } => {
            get_file_as_string_from_tar(file, "restore.sql")?
        }
    };

    let stmts = parser::parse_dump(&input).context("parsing dump")?;

    println!("importing");
    // TODO: probably only do one connection here instead of 2?

    println!("creating tables");
    create_tables_in_sqlite(
        stmts
            .iter()
            .filter(|x| matches!(x, Statement::CreateTable(_)))
            .collect_vec(),
        &sqlite_path,
    )?;

    println!("inserting data");
    insert_data_in_sqlite(
        stmts
            .iter()
            .filter(|x| matches!(x, Statement::Copy(_)))
            .collect_vec(),
        &dump_context,
        sqlite_path,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_import_from_sql_file_works() -> Result<()> {
        import_from_file("dump.sql", "test.db")?;

        todo!();
    }
}
