--load nyse data

nyse_data = load 'NYSE' as (exchange,symbol) --other fields not considered

--group by exhange

nyse_grpdata = group nyse_data by exchange;

--generate unige sybol count

nyse_uniqcount = foreach nyse_grpid {
					sym = nyse_data.symbol;
					unique_symbol = distince sym;
					generate group,COUNT(unique_symbol) as sym_count;
					};
					
-- dump nyse_uniqcount

dump nyse_uniqcount;