--piggybank scripts
--register pig

register '/home/bhuvan/bigdata/pig-0.12.0/piggybank.jar';
define lower org.apache.pig.piggybank.evaluation.string.LOWER();
emp_data = load '/home/bhuvan/pig/mycreateddata/empdata' using PigStorage(',') as (empno:chararray,fname:chararray,lname:chararray,company:chararray,salary:float);
lowername = foreach emp_data generate lower(fname),lower(lname);
dump lowername;

