mod parser;

use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use anyhow::{ensure, Context, Ok, Result};
use itertools::Itertools;
use parser::Statement;
use rusqlite;

enum DumpContext<P: AsRef<std::path::Path>> {
    Plain { sql_filepath: P },
    Tar { tar_filepath: P },
}

impl parser::ColumnType {
    fn to_sqlite_type(&self) -> &'_ str {
        match self {
            parser::ColumnType::Integer => "integer",
            parser::ColumnType::String => "text",
            parser::ColumnType::Boolean => "integer",
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

    match dump_context {
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
            let data = reader
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
                .collect_vec();

            Ok(data)
        }
        DumpContext::Tar { tar_filepath } => todo!(),
    }
}
fn insert_data_in_sqlite<P: AsRef<std::path::Path>>(
    stmts: Vec<&Statement>,
    dump_context: DumpContext<P>,
    sqlite_path: P,
) -> Result<()> {
    ensure!(stmts.iter().all(|x| matches!(x, Statement::Copy(_))));

    let mut conn = rusqlite::Connection::open(sqlite_path)?;

    for stmt in stmts {
        let sql_stmt = stmt_to_sql(stmt)?;
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

pub fn import_from_sql_file<P: AsRef<std::path::Path>>(sql_file: P, sqlite_path: P) -> Result<()> {
    let input = std::fs::read_to_string(&sql_file).context("reading sql file")?;

    let stmts = parser::parse_dump(&input).context("parsing dump")?;

    create_tables_in_sqlite(
        stmts
            .iter()
            .filter(|x| matches!(x, Statement::CreateTable(_)))
            .collect_vec(),
        &sqlite_path,
    )?;

    insert_data_in_sqlite(
        stmts
            .iter()
            .filter(|x| matches!(x, Statement::Copy(_)))
            .collect_vec(),
        DumpContext::Plain {
            sql_filepath: &sql_file,
        },
        &sqlite_path,
    )?;

    // context
    // dump type
    //

    let sql_stmts = stmts.iter().map(stmt_to_sql).collect::<Result<Vec<_>>>()?;
    println!("{:?}", sql_stmts.iter().format("\n"));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_import_from_sql_file_works() -> Result<()> {
        import_from_sql_file("dump.sql", "test.db")?;

        assert!(false);
        Ok(())
    }
}
