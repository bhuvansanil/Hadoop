--load emp data

emp_data = load 'empdata' as (empno,fname,lname,company,salary) using PigStorage(',');

--load personal data

personal_data = load 'personal' as (empno,fname,lanme,dob,age,address) using PigStorage(',');

--join using distributed cache of hadoop


join_data = join emp_data by empno,personal_data by empno using 'replicated';

-- dump join_data

dump join_data;

