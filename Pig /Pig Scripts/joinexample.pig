--load personal data

personal_data = load 'personal' using PigStorage(',') as (empno:chararray,fname:chararray,lname:chararray,dob:chararray,age:int,address:chararray);

--load employee data

emp_data = load 'empdata' using PigStorage(',') as (empno:chararray,fname:chararray,lname:chararray,company:chararray,salary:float);


--inner join emp_data with personal_data using empno as key

inner_joindata = join emp_data by empno,personal_data by empno;

--output
--(EMP0001,BHUVAN,SANIL,IBM,300000.0,EMP0001,BHUVAN,SANIL,12-03,1988,26)
--(EMP0002,BRIJESH,PANDAY,IBM,23043.03,EMP0002,BRIJESH,PANDAY,17-05,1988,26)
--(EMP0004,SHAYAN,DEVAIAH,CISCO,303943.22,EMP0004,SHAYAN,DEVEIAH,09-07-1986,28,VIRAJPET)

--inner join personal_data with emp_data using empno as key

inner_joindata = join personal_data by empno,emp_data by empno;

--output
--(EMP0001,BHUVAN,SANIL,12-03,1988,26,EMP0001,BHUVAN,SANIL,IBM,300000.0)
--(EMP0002,BRIJESH,PANDAY,17-05,1988,26,EMP0002,BRIJESH,PANDAY,IBM,23043.03)
--(EMP0004,SHAYAN,DEVEIAH,09-07-1986,28,VIRAJPET,EMP0004,SHAYAN,DEVAIAH,CISCO,303943.22)

--left outer joinn : left table is empdata;

left_outerjoin = join emp_data by empno left outer,personal_data by empno;

--output
--(EMP0001,BHUVAN,SANIL,IBM,300000.0,EMP0001,BHUVAN,SANIL,12-03,1988,26)
--(EMP0002,BRIJESH,PANDAY,IBM,23043.03,EMP0002,BRIJESH,PANDAY,17-05,1988,26)
--(EMP0003,NANJESH,KO,WELLS FARGO,30493.2,,,,,,)
--(EMP0004,SHAYAN,DEVAIAH,CISCO,303943.22,EMP0004,SHAYAN,DEVEIAH,09-07-1986,28,VIRAJPET)
--(EMP0005,JAGGA,DON,ATLM,2039084.2,,,,,,)

--right outer join : right table is personal_data

right_outerjoin = join emp_data by empno right outer,personal_data by empno;
--output
--(EMP0001,BHUVAN,SANIL,IBM,300000.0,EMP0001,BHUVAN,SANIL,12-03,1988,26)
--(EMP0002,BRIJESH,PANDAY,IBM,23043.03,EMP0002,BRIJESH,PANDAY,17-05,1988,26)
--(EMP0004,SHAYAN,DEVAIAH,CISCO,303943.22,EMP0004,SHAYAN,DEVEIAH,09-07-1986,28,VIRAJPET)
--(,,,,,EMP0007,ANANTHAMRAO,09-09-1987,22,KAMBIBANE,)
--(,,,,,EMP0008,SHASHI,KUMARA,08-07-1986,22,BETADABEEDY)

--self join

emp_data2 = load 'empdata' using PigStorage(',') as (empno:chararray,fname:chararray,lname:chararray,company:chararray,salary:float);
selfjoin = join emp_data by empno,emp_data2 by empno;

--output
--(EMP0001,BHUVAN,SANIL,IBM,300000.0,EMP0001,BHUVAN,SANIL,IBM,300000.0)
--(EMP0002,BRIJESH,PANDAY,IBM,23043.03,EMP0002,BRIJESH,PANDAY,IBM,23043.03)
--(EMP0003,NANJESH,KO,WELLS FARGO,30493.2,EMP0003,NANJESH,KO,WELLS FARGO,30493.2)
--(EMP0004,SHAYAN,DEVAIAH,CISCO,303943.22,EMP0004,SHAYAN,DEVAIAH,CISCO,303943.22)
--(EMP0005,JAGGA,DON,ATLM,2039084.2,EMP0005,JAGGA,DON,ATLM,2039084.2)






