--load nyse daily data
nyse_data = load '/pigdata/NYSE_daily' as (exchange:chararray,symbol:chararray,date:chararray,open:float,close:float,low:float,high:float,vol:long,avg:float);

--load nyse dividend data
nyse_dividend = load '/pigdata/NYSE_dividends' as (exchange:chararray,symbol:chararray,date:chararray,dividend:float);

--join nyse_data and nyse_dividednd based on symbol
nyse_joindata = join nyse_data by symbol,nyse_dividend by symbol; 

--retrive only required columns
nyse_savedata = foreach nyse_joindata generate $0,$1,$2,$3,$4,$5,$6,$7,$8,$12;

--save joined data

store nyse_savedata into '/stocksjoindata';


