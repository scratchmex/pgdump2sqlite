# pgdump2sqlite

![crates.io](https://img.shields.io/crates/v/pgdump2sqlite.svg)

use a [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html) to create a sqlite db

the objective is to use the dump AS IS. other solutions can be used but you need to strip the schemas like `public.table_name` from the statements.


## usage

```
pdump2sqlite pg_dump_file.<tar or sql> extracted_db.sqlite
```

```
Usage: pgdump2sqlite [OPTIONS] <PGDUMP_FILENAME> <SQLITE_FILENAME>

Arguments:
  <PGDUMP_FILENAME>  the file of the dump. can be .sql or .tar
  <SQLITE_FILENAME>  

Options:
  -f          delete the dst sqlite file if exists
  -h, --help  Print help
```

## benchmarks

for me, using a 16 MB tar dump with 39 tables and ~500K rows it takes 0.4 seconds. I would say pretty fast

## approach

1. use the [pest parser](https://github.com/pest-parser/pest) to get the statements
2. create all the tables
3. insert the data from the tar or sql file using prepared insert a transaction per table (for speed)

## support
- `create table` instruction
- `copy .. from <stdin or path>`
- Integer, Text, Boolean, Real dtypes in sqlite
- plain (`.sql`) or tar dump

## TODO

check the `// TODO:` comments

- support `insert into` statement (even tough this is not the default behavior and it takes much more space, don't do it)
- parse with pest using a buffer. see [pest: Support for streaming input](https://github.com/pest-parser/pest/issues/153)
- get rows for the copy lazily, don't read the whole file but use a generator (like in python) to return each row (I don't know how to do this)
- map `f` and `t` values to `0` and `1` in Bool dtype
- support `directory`, compressed tar and `custom` dump type
- have test data for the test (I have only locally but can't upload)


inspired by the scala version [postgresql-to-sqlite](https://github.com/caiiiycuk/postgresql-to-sqlite)
