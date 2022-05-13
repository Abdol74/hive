create database assign1;

create database assign1_loc
location '/hp_dp/assign1_loc' ;

use assign1;

CREATE  TABLE IF NOT EXISTS assign1.assign1_intern_tab(
     ID TINYINT ,
     NAME VARCHAR(25),
     AGE TINYINT,
     COUNTRY VARCHAR(25))
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;
	 

describe formatted assign1_intern_tab;

LOAD DATA LOCAL INPATH '/employee/employee_details.txt' INTO TABLE assign1_intern_tab;

!hdfs dfs -mkdir /course_demo;
!hdfs dfs -copyFromLocal -f /employee/employee_details.txt /course_demo ;
LOAD DATA INPATH '/course_demo/employee_details.txt' INTO TABLE assign1_intern_tab;

!hdfs dfs -put /employee/employee_details.txt /course_demo ;

create external table assign1_loc.assign1_intern_tab(
ID TINYINT,
NAME VARCHAR(25),
AGE TINYINT,
COUNTRY VARCHAR(25))
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/course_demo' ;


drop table assign1_loc.assign1_intern_tab;
drop table assign1.assign1_intern_tab;


CREATE TABLE IF NOT EXISTS assign1.staging(
ID TINYINT ,
NAME VARCHAR(25),
 AGE TINYINT,
 COUNTRY VARCHAR(25)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;

LOAD DATA  INPATH '/course_demo/employee_details.txt' INTO TABLE assign1.staging ;

create external table assign1_loc.assign1_intern_tab(
ID TINYINT,
NAME VARCHAR(25),
AGE TINYINT,
COUNTRY VARCHAR(25))
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;


CREATE  TABLE IF NOT EXISTS assign1.assign1_intern_tab(
     ID TINYINT ,
     NAME VARCHAR(25),
     AGE TINYINT,
     COUNTRY VARCHAR(25))
     ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;
	 

INSERT INTO assign1.assign1_intern_tab select * from assign1.staging ;

INSERT INTO assign1_loc.assign1_intern_tab select * from assign1.staging ;

create table assign1.songs(
artist_id STRING,
artist_latitude DOUBLE ,
artist_location STRING,
artist_longtitude DOUBLE,
artist_name STRING,
duration DOUBLE,
num_songs INT ,
song_id STRING,
title STRING,
year STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY  ','  lines terminated BY '\n'
tblproperties("skip.header.line.count"="1");


LOAD DATA  LOCAL INPATH '/employee/songs.csv' INTO TABLE assign1.songs ;

select count(*) from assign1.songs; 

drop table assign1.songs;


create table assign1.songs(
artist_id STRING,
artist_latitude DOUBLE ,
artist_location STRING,
artist_longtitude DOUBLE,
artist_name STRING,
duration DOUBLE,
num_songs INT ,
song_id STRING,
title STRING,
year STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';


create external table assign1.songs_ext(
artist_id STRING,
artist_latitude DOUBLE ,
artist_location STRING,
artist_longtitude DOUBLE,
artist_name STRING,
duration DOUBLE,
num_songs INT ,
song_id STRING,
title STRING,
year STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';


!hdfs dfs -mkdir /songs_data;
!hdfs dfs -put /employee/songs.csv /user/hive/warehouse/assign1.db/songs_ext ;

use assign1;
alter table assign1_intern_tab rename to  testdb.assign1_intern_tab;