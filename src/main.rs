use anyhow::Result;
use itertools::Itertools;
use std::fs;

fn create_table(fragment: &String) {
    // skip header
    let mut it = fragment
        .strip_prefix("create table")
        .expect(&format!("not a create table?: {}", fragment))
        .chars()
        .skip_while(|c| c.is_whitespace())
        .peekable();

    // table name
    let table_name: String = it
        .by_ref()
        .take_while(|c| !c.is_whitespace())
        // strip ticks or []
        .filter(|c| !['\'', '"', '[', ']'].contains(&c))
        .collect();

    println!("table name: {:?}", table_name);

    it.by_ref()
        .take_while(|&c| c.is_whitespace())
        .take_while(|&c| c == '(')
        .for_each(drop);

    // col names
    let mut col_names = Vec::<String>::new();
    while let Some(&cpeak) = it.peek() {
        // skip spaces
        if cpeak.is_whitespace() {
            it.next();
            continue;
        }

        // take until a new space
        let col_name: String = it
            .by_ref()
            .take_while(|c| !c.is_whitespace())
            // strip ticks or []
            .filter(|c| !['\'', '"', '[', ']'].contains(&c))
            .collect();

        // skip until comma or end of statement
        let _type: String = it.by_ref().take_while(|&c| c != ',').collect();

        // println!("{:?} has type {:?}", col_name, _type);

        col_names.push(col_name);
    }

    println!("col_names: {:?}", col_names);
}

fn copy_to_table<'a>(fragment: &String, lines: &mut impl Iterator<Item = &'a str>) {
    // extract table name
    let mut it = fragment
        .strip_prefix("copy")
        .expect(&format!("not copy?: {}", fragment))
        .chars()
        .peekable();

    let table_name = it
        .by_ref()
        .take_while(|&c| c != '(')
        .filter(|c| !['\'', '"', '[', ']'].contains(&c))
        .collect::<String>();
    let table_name = table_name.trim();

    println!("table_name: {:?}", table_name);

    let ncols = it
        .by_ref()
        .take_while(|&c| c != ')')
        .filter(|&c| c == ',')
        .count()
        + 1;

    println!("ncols: {}", ncols);

    let data = lines.take_while(|&l| l != r"\.");

    let data_rows = data
        .map(|entry| {
            entry
                .split("\t")
                .map(|e| match e {
                    r"\N" => "None",
                    _ => e,
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    println!("data rows: {:?}", data_rows.get(0));
}

fn main() -> Result<()> {
    let dump_file = "dump.sql";

    let content = fs::read_to_string(dump_file)?;

    let mut lines = content.split("\n").peekable();
    while let Some(&lpeak) = lines.by_ref().peek() {
        if lpeak.starts_with("--") || lpeak.is_empty() {
            lines.next();
            continue;
        }

        let fragment = lines
            .by_ref()
            .take_while_inclusive(|l| !l.ends_with(";"))
            .collect::<String>()
            .to_lowercase();

        if fragment.starts_with("create table") {
            create_table(&fragment);
        } else if fragment.starts_with("copy") {
            copy_to_table(&fragment, lines.by_ref());
            // break;
        } else {
            // println!("skipping {:?}", fragment);
            continue;
        }
    }

    Ok(())
}
