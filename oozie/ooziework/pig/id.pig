nyse_data = load '$INPUT' as (exchange:chararray,symbol:chararray,date:chararray,open:float,close:float,low:float,high:float,vol:long,avg:float);
nyse_cleandata = foreach nyse_data generate symbol,vol;
nyse_limit = limit nyse_cleandata 5;
store nyse_limit into '$OUTPUT' USING PigStorage();
