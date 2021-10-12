-- noinspection SqlNoDataSourceInspectionForFile

LOAD 'build/debug/sqlite_scanner.duckdb_extension';
select * from sqlite_scan('test.db', 'a')
