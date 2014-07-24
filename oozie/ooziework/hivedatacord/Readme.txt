this job loads the stcokdata into paritioned hive table every 10 minutes


Shell script is used to extract currentdate and time in required format\


table details

create external table stock_partition_table (
exchange_name string,
company_symbol string,
stock_date string,
open_price float,
high_price float,
low_price float,
close_price float,
stock_volume bigint,
avg_price float)
partitioned by (current_date_time string)
row format delimited fields terminated by '\t';


load data inpath '/data/NYSE_daily' into table stock_partition_table partition(current_date_time = ${MYDATE});
