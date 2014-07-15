--register UDFs
register '/home/bhuvan/workspace/MyPigUDFs.jar';
define maxvol com.bhuvan.pigudfs.MaxVolume2();


--load nyse data;

nyse_data = load '/stock' as (exchange:chararray,symbol:chararray,date:chararray,open:float,close:float,low:float,high:float,vol:int,avg1:float);

-- filter data;

nyse_filterdata = foreach nyse_data generate symbol,date,vol;

--group by symbol

nyse_grpdata = group nyse_filterdata by symbol;

--call UDF

nyse_udfdata = foreach nyse_grpdata generate group,maxvol(nyse_filterdata.date,nyse_filterdata.vol) as maxcol;

nyse_fltdata = foreach nyse_udfdata generate group,flatten(maxcol);



--dump data 
--fltdata = foreach nyse_udfdata generate group,flatten(STRSPLIT($1,','));

--dump nyse_udfdata;
--dump fltdata;
--xxx = foreach fltdata generate $0 as symbol,$1 as date,$2 as volume;

--describe fltdata;
--describe xxx;

--dump nyse_udfdata;
describe nyse_udfdata;
--dump nyse_fltdata;
--describe nyse_fltdata;

nyse_fltdata2 = foreach nyse_fltdata generate flatten($0) as symbol,flatten($1) as othersymbol,flatten($2) as date;;

dump nyse_fltdata2;