-- noinspection SqlNoDataSourceInspectionForFile
LOAD 'build/release/sqlite_scanner.duckdb_extension';
-- select * from sqlite_scan('borked.db', 'b') limit 10;
-- pragma threads=1;
-- select l_shipdate, l_discount from sqlite_scan('lineitem.db', 'lineitem') limit 10;

select count(*) from sqlite_scan('lineitem.db', 'lineitem');
.timer on

select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
from
--lineitem
  sqlite_scan('lineitem.db', 'lineitem')
where
  l_shipdate <= '1998-09-02'
group by
  l_returnflag,
  l_linestatus
order by
  l_returnflag,
  l_linestatus;