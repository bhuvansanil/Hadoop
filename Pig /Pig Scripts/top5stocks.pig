set default_parallel 10;
--load nyse data 

nyse_data = load '/stock' as (exchange:chararray,symbol:chararray,date:chararray,open:float,close:float,low:float,high:float,vol:long,avg:float);

--clean nyse_data for volume and symbol;

nyse_cleandata = foreach nyse_data generate symbol,vol;

--group nyse data by symbol

nyse_grpdata = group nyse_cleandata by symbol;

--sum volume

nyse_totalvol = foreach nyse_grpdata generate group,SUM(nyse_cleandata.vol);

--sort volume in descending order

nyse_sorted = order nyse_totalvol by $1 desc;

--limit top 5 stocks

nyse_limit = limit nyse_sorted 5;

--store nyse_limit

store nyse_limit into '/top5stocks' using PigStorage();

$PIG_HOME/pig -x local -e 'explain -script top5stocks.pig'

