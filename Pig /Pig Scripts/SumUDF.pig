--SUM UDF
--register UDF
register '/home/bhuvan/workspace/MyPigUDFs.jar';

define sumx com.bhuvan.pigudfs.PigAddition();

inputdata = load '/home/bhuvan/pig/pigdata/sumdata' using PigStorage(',') as (number1:int,number2:int);
sumdata = foreach inputdata generate sumx(number1,number2) as total;

dump sumdata;