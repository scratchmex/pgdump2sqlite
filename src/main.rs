use anyhow::Result;
use itertools::Itertools;
use rusqlite;
use std::fs;

struct ImportOp {
    connection: rusqlite::Connection,
}

impl ImportOp {
    pub fn new() -> Self {
        let connection = rusqlite::Connection::open("db.db").unwrap();

        ImportOp { connection }
    }

    pub fn parse(&mut self, content: String) -> Result<()> {
        let mut lines = content.split("\n").peekable();

        while let Some(&lpeak) = lines.by_ref().peek() {
            if lpeak.starts_with("--") || lpeak.is_empty() {
                lines.next();
                continue;
            }

            let fragment = lines
                .by_ref()
                .take_while_inclusive(|l| !l.ends_with(";"))
                .collect::<String>();

            if fragment.to_lowercase().starts_with("create table") {
                self.create_table(&fragment)?;
            } else if fragment.to_lowercase().starts_with("copy") {
                self.copy_to_table(&fragment, lines.by_ref())?;
            } else {
                // println!("skipping {:?}", fragment);
                continue;
            }
        }

        Ok(())
    }

    fn create_table(&self, fragment: &String) -> Result<()> {
        // "strip" the 'create table' statement
        let mut it = fragment
            .chars()
            .skip("create table".len())
            .skip_while(|c| c.is_whitespace())
            .peekable();

        // table name
        let table_name: String = it
            .by_ref()
            .take_while(|c| !c.is_whitespace())
            // strip ticks or []
            .filter(|c| !['\'', '"', '[', ']'].contains(&c))
            .collect();

        // println!("table name: {:?}", table_name);

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

            // skip until comma or end of statement, this is the type def
            // TODO: add typedef to schema
            let _type: String = it.by_ref().take_while(|&c| c != ',').collect();

            // println!("{:?} has type {:?}", col_name, _type);

            col_names.push(col_name);
        }

        // println!("col_names: {:?}", col_names);

        // create table [{tablename}] ( [{col1}], [{col2}], ... )
        let stmt = format!(
            "create table [{table_name}] ( {} )",
            col_names.iter().map(|c| format!("[{}]", c)).join(", ")
        );
        println!("{stmt:?}");

        self.connection.execute(&stmt, ())?;

        Ok(())
    }

    fn copy_to_table<'a>(
        &mut self,
        fragment: &String,
        lines: &mut impl Iterator<Item = &'a str>,
    ) -> Result<()> {
        // "strip" the copy statement
        let mut it = fragment.chars().skip("copy".len()).peekable();

        // extract table name
        let table_name = it
            .by_ref()
            .take_while(|&c| c != '(')
            .filter(|c| !['\'', '"', '[', ']'].contains(&c))
            .collect::<String>();
        let table_name = table_name.trim();

        // println!("table name: {:?}", table_name);

        // we parse the cols from statement be aware of the col order
        let col_names = it
            .by_ref()
            .take_while(|&c| c != ')')
            .collect::<String>()
            .split(",")
            .map(|c| c.trim())
            .map(|c| {
                c.chars()
                    .filter(|c| !['\'', '"', '[', ']'].contains(&c))
                    .collect()
            })
            .collect::<Vec<String>>();

        // println!("ncols: {}", col_names.len());

        // insert into [{table_name}] ( [{col1}], [{col2}], ... ) values ( ?, ?, ... )
        let stmt = format!(
            "insert into [{table_name}] ( {} ) values ( {} )",
            col_names.iter().map(|c| format!("[{}]", c)).join(", "),
            std::iter::repeat("?").take(col_names.len()).join(", ")
        );
        println!("{stmt:?}");

        let txn = self.connection.transaction()?;
        let mut prepared_insert = txn.prepare(&stmt)?;

        let data_rows = lines.take_while(|&l| l != r"\.").map(|entry| {
            entry
                .split("\t")
                // TODO: map on dependant type
                .map(|e| match e {
                    r"\N" => None,
                    // "f" => Some("false"),
                    // "t" => Some("true"),
                    _ => Some(e),
                })
                .collect::<Vec<_>>()
        });

        // TODO: print first row
        // println!("data rows: {:?}", data_rows.get(0));

        let mut rows_affected = 0;
        for row in data_rows {
            rows_affected += prepared_insert.execute(rusqlite::params_from_iter(row))?;
        }
        println!("^^ rows affected: {rows_affected}");

        drop(prepared_insert);
        txn.commit()?;

        Ok(())
    }
}

fn main() -> Result<()> {
    // TODO: don't read the whole file but use an interator
    let content = fs::read_to_string("dump.sql").unwrap();
    let mut import_op = ImportOp::new();

    println!("start");

    import_op.parse(content)?;

    Ok(())
}
