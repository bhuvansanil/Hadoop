--Parameter Substitution example 

nyse_data = load '/stock' as (exchange:chararray,symbol:chararray,date:chararray,open:float,close:float,low:float,high:float,vol:long,avg1:float); 
--filter data by 2009-12-31

filter_nyse_data = filter nyse_data by date == '$REQDATE';

dump filter_nyse_data;

--dump filter_nyse_data;

--dump nyse_data;

