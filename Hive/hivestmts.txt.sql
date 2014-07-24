CREATE DATABASE NYSE_STOCKS_DATA 
WITH DBPROPERTIES('creator' = 'bhuvan','date' = '12/03/1988')


---create table general synatx

CREATE TABLE table_name(column1 datatype,column2 datatype,...........,columnn datatype)
[ROW FORMAT DELIMITED]
FIELDS TERMINATED BY _____
[COLLECTIONS TERMINATED BY ____]
[MAP KEYS TERMINATED BY ____]
[LOCATION '/location']
[WITH SERDEPROPERTIES('schema url'='http://url)']
STORED AS 
[TEXTFILE,RCFILE,SEQUENCEFILE] =>default
[INPUTFORMAT 'package name']
[OUTPUTFORMAT 'package name'] 
[COMMENT "TABLE COMMENT"]
[TBLPROPRTTIES ('prop1' = 'value1','prop2'='value2',.....,'prop3'='value']


ALTER DATABASE NYSE_STOCKS_DATA SET DBPROPERTIES('name'='bhuvansanil','date'='12/03/1988');


--normal table
CREATE TABLE IF NOT EXISTS NYSE_TABLE (
	exchange_name STRING	COMMENT 'Exchange name',
	symbol STRING	COMMENT 'Symbol name',
	date STRING	COMMENT 'date',
	open FLOAT	COMMENT 'Open price',
	high FLOAT	COMMENT 'High price from open',
	low FLOAT	COMMENT 'Low price from open',
	close FLOAT 	COMMENT 'close price',
	volume BIGINT	COMMENT 'stock volume',
	avg_price FLOAT	COMMENT 'average stock price')
	COMMENT 'Description of table'
	TBLPROPERTIES('creator'='bhuvan','creation_date'='2014-07-12');



-- complex data type table

create table collection_table2 (                                        
     name string comment 'user name',
     age int comment 'user age',
     subordinates array<string> comment 'subordinates under user',
     salary map<string,float> comment 'salary descriptions',
     address struct<dno:int,street:string,city:string> comment 'user address')
     comment 'user table description'     
     row format delimited fields terminated by ','
     collection items terminated by '|'
     map keys terminated by ':'
     tblproperties ('creator'='bhuvan','creation_Date'='12-03-1988');
     [location]
/*
--data sample

bhuvan,26,vinu|ramu|choma|yam,pf:2000|total:60000,62|tilaknagar|banglore
panday,26,vinu|tamu|thoma|tam,pf:3000|total:70000,63|hsr|banglore
vinod,26,vids|rasdu|cafdoma|sadam,pf:12000|total:230000,62|tilaknagar|banglore*/

--data output
/*
bhuvan	26	["vinu","ramu","choma","yam"]	{"pf":2000.0,"total":60000.0}	{"dno":62,"street":"tilaknagar","city":"banglore"}
panday	26	["vinu","tamu","thoma","tam"]	{"pf":3000.0,"total":70000.0}	{"dno":63,"street":"hsr","city":"banglore"}
vinod	26	["vids","rasdu","cafdoma","sadam"]	{"pf":12000.0,"total":230000.0}	{"dno":62,"street":"tilaknagar","city":"banglore"}
bhuvan	26	["vinu","ramu","choma","yam"]	{"pf":2000.0,"total":60000.0}	{"dno":62,"street":"tilaknagar","city":"banglore"}
panday	26	["vinu","tamu","thoma","tam"]	{"pf":3000.0,"total":70000.0}	{"dno":63,"street":"hsr","city":"banglore"}
vinod	26	["vids","rasdu","cafdoma","sadam"]	{"pf":12000.0,"total":230000.0}	{"dno":62,"street":"tilaknagar","city":"banglore"}
bhuvan	26	["vinu","ramu","choma","yam"]	{"pf":2000.0,"total":60000.0}	{"dno":62,"street":"tilaknagar","city":"banglore"}
panday	26	["vinu","tamu","thoma","tam"]	{"pf":3000.0,"total":70000.0}	{"dno":63,"street":"hsr","city":"banglore"}
vinod	26	["vids","rasdu","cafdoma","sadam"]	{"pf":12000.0,"total":230000.0}	{"dno":62,"street":"tilaknagar","city":"banglore"}*/

--load data from local

load data local inpath '/home/bhuvan/collectiondata' into table collection_table2;    


--load data from hdfs path

load data inpath '/home/bhuvan/collectiondata' into table collection_table2;    


--describe table 
describe collection_table2;

name                	string              	user name           
age                 	int                 	user age            
subordinates        	array<string>       	subordinates under user
salary              	map<string,float>   	salary descriptions 
address             	struct<dno:int,street:string,city:string>	user address        
Time taken: 0.068 seconds, Fetched: 5 row(s)


--accessing array elements

select * from collection_table2 where subordinates[0] = "vinu";

--output

bhuvan	26	["vinu","ramu","choma","yam"]	{"pf":2000.0,"total":60000.0}	{"dno":62,"street":"tilaknagar","city":"banglore"}
panday	26	["vinu","tamu","thoma","tam"]	{"pf":3000.0,"total":70000.0}	{"dno":63,"street":"hsr","city":"banglore"}
bhuvan	26	["vinu","ramu","choma","yam"]	{"pf":2000.0,"total":60000.0}	{"dno":62,"street":"tilaknagar","city":"banglore"}

-- accessing map elements

select * from collection_table2 where salary["total"] = 60000.0;

--output

bhuvan	26	["vinu","ramu","choma","yam"]	{"pf":2000.0,"total":60000.0}	{"dno":62,"street":"tilaknagar","city":"banglore"}
bhuvan	26	["vinu","ramu","choma","yam"]	{"pf":2000.0,"total":60000.0}	{"dno":62,"street":"tilaknagar","city":"banglore"}
bhuvan	26	["vinu","ramu","choma","yam"]	{"pf":2000.0,"total":60000.0}	{"dno":62,"street":"tilaknagar","city":"banglore"}

--accessing struct elements

select * from collection_table2 where address.dno = 63;


--output

panday	26	["vinu","tamu","thoma","tam"]	{"pf":3000.0,"total":70000.0}	{"dno":63,"street":"hsr","city":"banglore"}
panday	26	["vinu","tamu","thoma","tam"]	{"pf":3000.0,"total":70000.0}	{"dno":63,"street":"hsr","city":"banglore"}
panday	26	["vinu","tamu","thoma","tam"]	{"pf":3000.0,"total":70000.0}	{"dno":63,"street":"hsr","city":"banglore"}


--truncate table

truncate table collection_table2;


--drop table

drop table collection_table2;

-------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------

PARTITIONING TABLES

1) STATIC PARTITIONING:(virtual:field)


create table sock_partitioned_table (
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

--rename table 

alter table sock_partitioned_table rename to stock_partition_table;

--describe table

exchange_name       	string              	None                
company_symbol      	string              	None                
stock_date          	string              	None                
open_price          	float               	None                
high_price          	float               	None                
low_price           	float               	None                
close_price         	float               	None                
stock_volume        	bigint              	None                
avg_price           	float               	None                
current_date_time   	string              	None                
	 	 
# Partition Information	 	 
# col_name            	data_type           	comment             
	 	 
current_date_time   	string              	None          


--load data into partition table

-- used shell script to get the date and append it to current_date_time

#!/bin/bash
DATE_YEST=`TZ=GMT+48 date +%Y%m%d%H%M%S`
echo $DATE_YEST
hive -S -e "load data local inpath '/home/bhuvan/pig/pigdata/NYSE_daily' into table nyse_stock_data.stock_partition_table partition(current_date_time = '$DATE_YEST');"

--sample query

select * from stock_partition_table where current_date_time = '20140712190212' limit 3; 
/*OK
NYSE	CLI	2009-12-31	35.39	35.7	34.5	34.57	890100	34.12	20140712190212
NYSE	CLI	2009-12-30	35.22	35.46	34.96	35.4	516900	34.94	20140712190212
NYSE	CLI	2009-12-29	35.69	35.95	35.21	35.34	556500	34.88	20140712190212
Time taken: 0.114 seconds, Fetched: 3 row(s)

--select company_symbol,sum(open_price) from stock_partition_table group by company_symbol,current_date_time limit 5;

CA	5038.920000076294
CA	5038.920000076294
CA	5038.920000076294
CAB	2957.409996509552
CAB	2957.409996509552
--------------------------------------------------
-- DYNAMIC PARTITIONING*/


create table stock_dynamic_partition(exchange_name string,
stock_date string,
open_price float,
high_price float,
low_price float,
close_price float,
stock_volume bigint,
avg_price float)
partitioned by (company_name string)
row format delimited fields terminated by '/t';

--insert data ;

insert into table stock_dynamic_partition partition(company_name) select exchange_name,date,open,high,low,close,volume,avg_price,symbol from nyse_stock_data2;


alter table stock_dynamic_partition add partition(sys_data string);

------------------------------

alter 

--change colums 
ALTER TABLE table_name CHANGE COLUMN oldcolumnname newcolumname datatype [COMMENT] [[AFTER column name]=>move column after column namde[FIRST] =>move column to first];





--some sampple quries

select symbol,date,round(low /2) from nyse_stock_data2 where symbol = 'CVA'; 
select symbol,date,round(low /2) from nyse_stock_data2 where symbol = upper('cva');

select count(*) from nyse_stock_data2;  

select count(DISTINCT symbol) from nyse_stock_data2;


-- create table nyse_dividends

create table nyse_stock_dividends (
exchange_name string,
company_symbol string,
stock_date string,
dividend_given float)
row format delimited fields terminated by '\t';


load data local inpath '/home/bhuvan/pig/pigdata/NYSE_dividends' into table nyse_stock_dividends;

--group by and having
select company_symbol,sum(dividend_given) from nyse_stock_dividends group by company_symbol having avg(dividend_given) < 2;

JOINS

--INNER JOIB FULL OUTER join

--MAPSIDE JOINS(USED FOR OPTIMIZATION) 

SELECT /*+ MAPJOIN(d) */ s.symbol,s.open,d.dividend_given from nyse_stock_data s join nyse_stock_dividends d on s.symbol = d.company_symbol;

--ORDER BY AND SORT BY

--ORDER BY (TOTAL SORT - all records are passed to single reducer -Perfomance low)
--SORT BY (LOCAL SORT - sorted in different reducers)

select * from nyse_stock_data order by open desc;

select * from nyse_stock_data sort by open desc;

--DISTRIBUTE BY

/*while using sort the redords will go to differer reduces so for example if we sort the stock data on basis of open price using SORT BY, the we may ge output like

AAP 12.0
AAP 13.9
CLI 14.0
CLA 15.0
AAP 16.0 => gone to another reducer
CLI 17.0 => gonr to another reducer

to overcome this we use DISTRIBUTE BY so that we get the stock number in sequence*/

SELECT symbol,open from nyse_stock_data distribute by symbol sort by open;


--CLUSTER BY

select symbol,open from nyse_stock_data cluster by symbol;

--CASTING

select symbol,open from nyse_stock_data where cast(volume as int) < 10000;

---UNIon

select * from nyse_stock_data union all select * from nyse_stock_dividends -> not supported

select log.symbol from (
select symbol from nyse_stock_data UNION ALL select company_symbol from nyse_stock_dividends) ;

alter table stock_bucket_table change column volume volume string;  

--VIEWS

create view stock_view as 
select nyse_stock_data.open,
nyse_stock_data.close,
nyse_stock_dividends.dividend_given,
nyse_stock_data.symbol from nyse_stock_data  join nyse_stock_dividends  on 
(nyse_stock_data.symbol = nyse_stock_dividends.company_symbol) where nyse_stock_data.open > 2.0;

--INDEX

create index stock_partition_index on table stock_partition_table(company_symbol)
as 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
with deferred rebuild;

--show index

hive> show formatted index on stock_partition_table;

--BUCKETINg

create table stock_bucket_table(exchange_name string,
company_symbol string,
market_date string,
open_price float,
high_price float,
low_price float,
close_price float,
volume bigint,
avg_price float)
clustered by (company_symbol) into 237 buckets;

--insert table

insert into table stock_bucket_table select * from nyse_stock_data;

--alter table change columns
alter table stock_bucket_table change column volume volume string;  



