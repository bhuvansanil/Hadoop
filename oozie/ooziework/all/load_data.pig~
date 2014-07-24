loaddata = load '$INPUT' as (cname:chararray,ename:chararray,volume:chararray);
limit_data = limit loaddata 5;  
names = load '$JDATA' using PigStorage(',') as (ssymbol:chararray,sname:chararray);
joindata = join limit_data by cname,names by ssymbol;
finaldata = foreach joindata generate $1,$4,$2;
store finaldata into '$OUTPUT' USING PigStorage();


