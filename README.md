# DuckDB sqlitescanner extension

The sqlitescanner extension allows DuckDB to directly read data from a SQLite database file. The data can be queried directly from the underlying SQLite tables, or read into DuckDB tables.

## Usage

To make a SQLite file accessible to DuckDB, use the `ATTACH` command, for example with the bundled `sakila.db` file:
```SQL
CALL sqlite_attach('sakila.db');
```

The tables in the file are registered as views in DuckDB, you can list them with
```SQL
PRAGMA show_tables;
```

Then you can query those views normally using SQL, e.g. using the example queries from sakila-examples.sql

```
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


Or run them all in both SQLite and DuckDB
```bash
./duckdb/build/release/duckdb < sakila-examples.sql
sqlite3 sakila.db < sakila-examples.sql
```

## Building & Loading the Extension

To build, type 
```
make duckdb_release release
```

To run, run the bundled `duckdb` shell:
```
 ./duckdb/build/release/duckdb 
```

Then, load the SQLite extension like so:
```SQL
LOAD 'build/release/sqlite_scanner.duckdb_extension';
```

