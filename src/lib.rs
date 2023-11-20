mod parser;

use anyhow::{ensure, Context, Ok, Result};
use itertools::Itertools;
use parser::Statement;
use rusqlite;

enum DumpFormat {
    Plain { sql_filepath: String },
    Tar { tar_filepath: String },
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

pub fn import_from_sql_file<P: AsRef<std::path::Path>>(sql_file: P, sqlite_path: P) -> Result<()> {
    let input = std::fs::read_to_string(sql_file).context("reading sql file")?;

    let stmts = parser::parse_dump(&input).context("parsing dump")?;

    create_tables_in_sqlite(
        stmts
            .iter()
            .filter(|x| matches!(x, Statement::CreateTable(_)))
            .collect_vec(),
        sqlite_path,
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
        import_from_sql_file("restore.sql", "test.db")?;

        assert!(false);
        Ok(())
    }
}
