--Macro Example

define dividend_analysis(daily,year,daily_symbol,dopen,dclose)
returns analyzed{
	divs = load '/pigdata/NYSE_dividends' as (exchange,symbol,date,div);
	divthisyear = filter divs by date matches '2009-.*';
	dailythisyear = filter $daily by date matches '2009-.*';
	joindata = join divthisyear by symbol,dailythisyear by $daily_symbol;
	--$analyzed = foreach joindata generate dailythisyear::daily_symbol,divthisyear::div;
	--describe joindata;	
	$analyzed = foreach joindata generate divthisyear::symbol,dailythisyear::$dopen; 
		};
	
	daily = load '/pigdata/NYSE_daily' as (exchange,symbol,date,open,close,wlow,whigh,vol,avg1);
	results = dividend_analysis(daily,'2009',symbol,open,close);
	top5 = limit results 5;
	dump top5;
	describe results;