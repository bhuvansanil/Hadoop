--load nyse daily data
nyse_data = load '/pigdata/NYSE_daily' as (exchange:chararray,symbol:chararray,date:chararray,open:float,close:float,low:float,high:float,vol:long,avg1:float);

--project only required columns

nyse_projdata = foreach nyse_data generate (symbol,date,open,close,low,high,vol,avg1);

--call MR

mrout = mapreduce 'workspace/PigMR.jar' 
		store nyse_projdata into '/input'
		load 'output' 
		`com.pig.mr.StockMaxVolume -i /input -o /output`;

store mrout into '/outesd12234';
