DROP TABLE IF EXISTS assign1.assign1_intern_tab ; 
use assign1;
CREATE  TABLE IF NOT EXISTS assign1.assign1_intern_tab(
ID TINYINT ,
NAME VARCHAR(25),
AGE TINYINT,
COUNTRY VARCHAR(25))
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;
LOAD DATA LOCAL INPATH '/employee/employee_details.txt' INTO TABLE assign1_intern_tab;