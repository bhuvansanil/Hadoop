USE stock_database;
load data local inpath '/home/hduser/pigdata/NYSE_daily' into table stock_partition_table partition(current_date_time = ${MYDATE});
