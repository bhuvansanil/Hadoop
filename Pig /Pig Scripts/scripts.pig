grunt> nyse_data = load 'NYSE' as (exchange:chararray,symbol:chararray,date:chararray,open:float,close:float,wlow:float,whigh:float,vol:int,avg:float);

grunt> dump nyse_data;

grunt> openclose = foreach nyse_data generate symbol,open,close,vol;

grunt>describe openclose;

grunt> gain = foreach openclose generate open - close;


/* data is 
(-0.31000137)
(-0.23999786)
(-0.26000214)
(-0.5600014)
(-0.2199974)
*/

grunt> tillclose = foreach nyse_data generate ..close;

/*output data is 

(NYSE,CLI,2009-12-31,35.39,35.7)
(NYSE,CLI,2009-12-30,35.22,35.46)
(NYSE,CL2,2009-12-29,35.69,35.95)
(NYSE,CL2,2009-12-28,35.67,36.23)
(NYSE,CL4,2009-12-24,35.38,35.6)

*/


grunt> fromsymboltoclose = foreach nyse_data generate symbol..close;

/*output data is 

CLI,2009-12-31,35.39,35.7)
(CLI,2009-12-30,35.22,35.46)
(CL2,2009-12-29,35.69,35.95)
(CL2,2009-12-28,35.67,36.23)
(CL4,2009-12-24,35.38,35.6)
*/

/* casting example

grunt> aa = foreach nyse_data generate (int) open / (int) close;

/* output data

(1)
(1)
(1)
(0)
(1)
*/

grunt> aa = foreach nyse_data generate  (int)(open / close);

/*output
(0)
(0)
(0)
(0)
(0)
*/

/* bitcond operator

 xx = foreach nyse_data generate symbol,(open > close ? 0 : 1) as temp;

output is 
(CLI,1)
(CLI,1)
(CL2,1)
(CL2,1)
(CL4,1)
*/







