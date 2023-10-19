# DuckDB SQLite extension

The SQLite extension allows DuckDB to directly read and write data from a SQLite database file. The data can be queried directly from the underlying SQLite tables. Data can be loaded from SQLite tables into DuckDB tables, or vice versa.

## Reading Data from SQLite

To make a SQLite file accessible to DuckDB, use the `ATTACH` command, for example with the bundled `sakila.db` file:

```sql
ATTACH 'data/db/sakila.db' AS sakila;
USE sakila;
```

The tables in the file can be read as if they were normal DuckDB tables, but the underlying data is read directly from the SQLite tables in the file at query time.

```sql
SHOW TABLES;
```

You can query the tables using SQL, e.g. using the example queries from sakila-examples.sql

```sql
SELECT cat.name category_name, 
       Sum(Ifnull(pay.amount, 0)) revenue 
FROM   category cat 
       LEFT JOIN film_category flm_cat 
              ON cat.category_id = flm_cat.category_id 
       LEFT JOIN film fil 
              ON flm_cat.film_id = fil.film_id 
       LEFT JOIN inventory inv 
              ON fil.film_id = inv.film_id 
       LEFT JOIN rental ren 
              ON inv.inventory_id = ren.inventory_id 
       LEFT JOIN payment pay 
              ON ren.rental_id = pay.rental_id 
GROUP  BY cat.name 
ORDER  BY revenue DESC 
LIMIT  5; 
```

## Opening SQLite Databases Directly

SQLite databases can also be opened directly and can be used transparently instead of a DuckDB database file. In any client, when connecting, a path to a SQLite database file can be provided and the SQLite database will be opened instead.

For example, with the shell:

```sql
$ > duckdb data/db/sakila.db 
v0.9.1 401c8061c6
D SHOW tables;
┌────────────────────────┐
│          name          │
│        varchar         │
├────────────────────────┤
│ actor                  │
│ address                │
│ category               │
│ city                   │
│ country                │
│ customer               │
│ customer_list          │
│ film                   │
│ film_actor             │
│ film_category          │
│ film_list              │
│ film_text              │
│ inventory              │
│ language               │
│ payment                │
│ rental                 │
│ sales_by_film_category │
│ sales_by_store         │
│ staff                  │
│ staff_list             │
│ store                  │
├────────────────────────┤
│        21 rows         │
└────────────────────────┘
```

## Writing Data to SQLite

In addition to reading data from SQLite, the extension also allows you to create new SQLite database files, create tables, ingest data into SQLite and make other modifications to SQLite database files using standard SQL queries.

This allows you to use DuckDB to, for example, export data that is stored in a SQLite database to Parquet, or read data from a Parquet file into SQLite.

Below is a brief example of how to create a new SQLite database and load data into it.

```sql
ATTACH 'new_sqlite_database.db' AS sqlite_db (TYPE SQLITE);
CREATE TABLE sqlite_db.tbl(id INTEGER, name VARCHAR);
INSERT INTO sqlite_db.tbl VALUES (42, 'DuckDB');
```

The resulting SQLite database can then be read into from SQLite.

```sql
$r > sqlite3 new_sqlite_database.db 
SQLite version 3.39.5 2022-10-14 20:58:05
sqlite> SELECT * FROM tbl;
id  name  
--  ------
42  DuckDB
```

Many operations on SQLite tables are supported. All these operations directly modify the SQLite database, and the result of subsequent operations can then be read using SQLite.

Below is a list of supported operations.

###### CREATE TABLE
```sql
CREATE TABLE sqlite_db.tbl(id INTEGER, name VARCHAR);
```

###### INSERT INTO
```sql
INSERT INTO sqlite_db.tbl VALUES (42, 'DuckDB');
```

###### SELECT
```sql
SELECT * FROM sqlite_db.tbl;
┌───────┬─────────┐
│  id   │  name   │
│ int64 │ varchar │
├───────┼─────────┤
│    42 │ DuckDB  │
└───────┴─────────┘
```

###### COPY
```sql
COPY sqlite_db.tbl TO 'data.parquet';
COPY sqlite_db.tbl FROM 'data.parquet';
```

###### UPDATE
```sql
UPDATE sqlite_db.tbl SET name='Woohoo' WHERE id=42;
```

###### DELETE
```sql
DELETE FROM sqlite_db.tbl WHERE id=42;
```

###### ALTER TABLE
```sql
ALTER TABLE sqlite_db.tbl ADD COLUMN k INTEGER;
```

###### DROP TABLE
```sql
DROP TABLE sqlite_db.tbl;
```

###### CREATE VIEW
```sql
CREATE VIEW sqlite_db.v1 AS SELECT 42;
```

###### Transactions
```sql
CREATE TABLE sqlite_db.tmp(i INTEGER);
BEGIN;
INSERT INTO sqlite_db.tmp VALUES (42);
SELECT * FROM sqlite_db.tmp;
┌───────┐
│   i   │
│ int64 │
├───────┤
│    42 │
└───────┘
ROLLBACK;
SELECT * FROM sqlite_db.tmp;
┌────────┐
│   i    │
│ int64  │
├────────┤
│ 0 rows │
└────────┘
```

## Building & Loading the Extension

To build, type 
```
make
```

To run, run the bundled `duckdb` shell:
```
 ./build/release/duckdb -unsigned
```

Then, load the SQLite extension like so:
```SQL
LOAD 'build/release/extension/sqlite_scanner/sqlite_scanner.duckdb_extension';
```

