--register UDFs
register '/home/bhuvan/workspace/MyPigUDFs.jar';
define maxvol com.bhuvan.pigudfs.MaxVolume();


--load nyse data;

nyse_data = load '/stock' as (exchange:chararray,symbol:chararray,date:chararray,open:float,close:float,low:float,high:float,vol:int,avg1:float);

-- filter data;

nyse_filterdata = foreach nyse_data generate symbol,date,vol;

--group by symbol

nyse_grpdata = group nyse_filterdata by symbol;

--call UDF

nyse_udfdata = foreach nyse_grpdata generate group,maxvol(nyse_filterdata.date,nyse_filterdata.vol);

--dump data 
fltdata = foreach nyse_udfdata generate group,flatten(STRSPLIT($1,','));

--dump nyse_udfdata;
dump fltdata;
xxx = foreach fltdata generate $0 as symbol,$1 as date,$2 as volume;

describe fltdata;
describe xxx;