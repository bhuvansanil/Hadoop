USE stock_database;
load data inpath '/user/hduser/pig-outdata/part*' into table stock_main_table;
