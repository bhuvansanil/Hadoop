USE stock_database;
drop table stock_main_table;
create table if not exists stock_main_table (e_name string,cname string,volume string) row format delimited fields terminated by '\t'; 
