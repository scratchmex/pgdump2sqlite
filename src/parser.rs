use anyhow::{ensure, Context, Ok, Result};
use itertools::Itertools;
use pest::{iterators::Pair, Parser};
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "pgdump.pest"]
struct PgdumpParser;

impl PgdumpParser {
    fn new() -> Self {
        Self {}
    }
}

fn parse_value(pair: Pair<Rule>) -> Result<String> {
    ensure!(pair.as_rule() == Rule::value);

    let inner = pair.into_inner().next().context("inner")?;

    Ok(match inner.as_rule() {
        Rule::unquoted_val => inner.as_str().to_string(),
        Rule::quoted_val => inner
            .into_inner()
            .next()
            .expect("quoted_inner")
            .as_str()
            .to_string(),
        Rule::bracketed_val => inner
            .into_inner()
            .next()
            .expect("bracketed_inner")
            .as_str()
            .to_string(),
        _ => unreachable!(),
    })
}

#[derive(Debug, PartialEq)]
enum ColumnType {
    Integer,
    String,
}

struct Column {
    name: String,
    dtype: ColumnType,
}

fn parse_column_def(pair: Pair<Rule>) -> Result<Column> {
    assert_eq!(pair.as_rule(), Rule::column_def);

    let mut pair_inner = pair.into_inner();

    let column_name = parse_value(
        pair_inner
            .next()
            .expect("column_name")
            .into_inner()
            .next()
            .expect("value"),
    )?;
    let column_type = pair_inner.next().expect("column_type").as_str();

    Ok(Column {
        name: column_name,
        dtype: match column_type {
            "integer" => ColumnType::Integer,
            _ => ColumnType::String,
        },
    })
}

fn parse_table_def(pair: Pair<Rule>) -> Result<String> {
    let value = pair
        .into_inner()
        .filter(|x| x.as_rule() == Rule::table_name)
        .next()
        .expect("table_name")
        .into_inner()
        .next()
        .expect("value");

    parse_value(value)
}

pub struct CreateTable {
    name: String,
    columns: Vec<Column>,
}

fn parse_create_table_stmt(pair: Pair<Rule>) -> Result<CreateTable> {
    assert_eq!(pair.as_rule(), Rule::create_table_stmnt);

    let mut pair_inner = pair.into_inner();

    let table_def = pair_inner.next().expect("table_def");
    let table_fields = pair_inner.next().expect("table_fields");

    let table_name = parse_table_def(table_def)?;

    let column_defs = table_fields
        .into_inner()
        .map(parse_column_def)
        .collect::<Result<Vec<_>>>()
        .context("collecting col defs")?;

    Ok(CreateTable {
        name: table_name,
        columns: column_defs,
    })
}

pub struct CopyTable {
    name: String,
    columns: Vec<String>,
    from: String,
}

fn parse_copy_stmt(pair: Pair<Rule>) -> Result<CopyTable> {
    let mut pair_inner = pair.into_inner();

    let table_def = pair_inner.next().expect("table_def");
    let column_names = pair_inner.next().expect("column_names");
    let from_val = pair_inner.next().expect("from_val");

    let table_name = parse_table_def(table_def)?;
    let columns = column_names
        .into_inner()
        .flat_map(|x| x.into_inner())
        .map(|x| parse_value(x))
        .collect::<Result<Vec<_>>>()
        .context("collecting col names")?;
    let from = parse_value(from_val.into_inner().next().expect("value"))?;

    Ok(CopyTable {
        name: table_name,
        columns: columns,
        from,
    })
}

pub enum Statement {
    CreateTable(CreateTable),
    Copy(CopyTable),
}

fn parse_stmt(pair: Pair<Rule>) -> Result<Option<Statement>> {
    Ok(match pair.as_rule() {
        Rule::create_table_stmnt => Some(Statement::CreateTable(parse_create_table_stmt(pair)?)),
        Rule::copy_stmnt => Some(Statement::Copy(parse_copy_stmt(pair)?)),
        Rule::unsupported => None,
        _ => unreachable!(),
    })
}

pub fn parse_dump(input: &'_ str) -> Result<Vec<Statement>> {
    let main = PgdumpParser::parse(Rule::main, &input).expect("parse failed");

    let stmts = main
        .into_iter()
        .take_while(|x| x.as_rule() != Rule::EOI)
        .map(|x| parse_stmt(x))
        .filter_map_ok(|x| x)
        .collect::<Result<Vec<_>>>()?;

    Ok(stmts)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_create_table() -> Result<()> {
        let stmt_in = r#"CREATE TABLE public.session (
id integer NOT NULL,
date timestamp without time zone DEFAULT now() NOT NULL,
turn public.session_turn_enum NOT NULL,
role public.user_role_enum DEFAULT 'other'::public.user_role_enum NOT NULL,
"userId" integer
);
"#;
        let stmt = PgdumpParser::parse(Rule::statement, stmt_in)
            .expect("parse failed")
            .next()
            .expect("statement");

        assert_eq!(stmt.as_rule(), Rule::create_table_stmnt);

        let create_table = match parse_stmt(stmt)? {
            Some(Statement::CreateTable(ct)) => ct,
            Some(Statement::Copy(_)) => unreachable!(),
            None => unreachable!(),
        };

        assert_eq!(create_table.name, "session");

        assert_eq!(
            create_table
                .columns
                .iter()
                .map(|x| &x.name)
                .collect::<Vec<_>>(),
            ["id", "date", "turn", "role", "userId"]
        );

        assert_eq!(
            create_table
                .columns
                .iter()
                .map(|x| &x.dtype)
                .collect::<Vec<_>>(),
            [
                &ColumnType::Integer,
                &ColumnType::String,
                &ColumnType::String,
                &ColumnType::String,
                &ColumnType::Integer
            ]
        );

        Ok(())
    }

    #[test]
    fn test_parse_copy() -> Result<()> {
        let stmt_in = r#"COPY public.attendance (
    id,
    "checkIn",
    "checkOut",
    active,
    "entryInventoryId",
    "exitInventoryId",
    "userId",
    "cashMachineId",
    "fundsTransferedId"
) FROM '$$PATH$$/3399.dat';
"#;
        let stmt = PgdumpParser::parse(Rule::statement, stmt_in)
            .expect("parse failed")
            .next()
            .expect("statement");

        assert_eq!(stmt.as_rule(), Rule::copy_stmnt);

        let copy_table = match parse_stmt(stmt)? {
            Some(Statement::Copy(ct)) => ct,
            Some(Statement::CreateTable(_)) => unreachable!(),
            None => unreachable!(),
        };

        assert_eq!(copy_table.name, "attendance");

        assert_eq!(
            copy_table.columns,
            [
                "id",
                "checkIn",
                "checkOut",
                "active",
                "entryInventoryId",
                "exitInventoryId",
                "userId",
                "cashMachineId",
                "fundsTransferedId"
            ]
        );

        assert_eq!(copy_table.from, "$$PATH$$/3399.dat");

        Ok(())
    }

    #[test]
    fn test_parse_stmt_with_comment() -> Result<()> {
        let stmt_in = r#"--
-- NOTE:
--
-- File paths need to be edited. Search for $$PATH$$ and
-- replace it with the path to the directory containing
-- the extracted data files.
--
--
-- PostgreSQL database dump
--

-- Dumped from database version 13.4 (Debian 13.4-1.pgdg100+1)
-- Dumped by pg_dump version 13.4 (Debian 13.4-1.pgdg100+1)


COPY public.attendance (id, "checkIn", "checkOut", active, "entryInventoryId", "exitInventoryId", "userId", "cashMachineId", "fundsTransferedId") FROM stdin;
"#;
        let stmt = PgdumpParser::parse(Rule::main, stmt_in)
            .expect("parse failed")
            .next()
            .expect("statement");

        assert_eq!(stmt.as_rule(), Rule::copy_stmnt);

        Ok(())
    }

    #[test]
    fn test_it_works_on_a_dump() -> Result<()> {
        let input = std::fs::read_to_string("restore.sql").expect("restore.sql file to test");

        let stmts = parse_dump(&input)?;

        assert!(!stmts.is_empty());

        Ok(())
    }
}
