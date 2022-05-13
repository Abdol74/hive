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

create database assign2;

create external table assign2.partitioned_songs(
artist_id STRING,
artist_latitude DOUBLE ,
artist_location STRING,
artist_longtitude DOUBLE,
duration DOUBLE,
num_songs INT ,
song_id STRING,
title STRING)
partitioned by (artist_name STRING, year STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';


create  table assign2.songs(
artist_id STRING,
artist_latitude DOUBLE ,
artist_location STRING,
artist_longtitude DOUBLE,
duration DOUBLE,
num_songs INT ,
song_id STRING,
title STRING)
partitioned by (year STRING , artist_name STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

!hdfs dfs -put /employee/songs.csv /user/hive/warehouse/assign2.db/partitioned_songs;

!hdfs dfs -put /employee/songs.csv /user/hive/warehouse/assign2.db/songs;


ALTER TABLE partitioned_songs ADD
PARTITION (artist_name='AR8IEZO1187B99055E',year='2008')
LOCATION '/partitions_page_views' ;

ALTER TABLE songs ADD PARTITION (artist_name='AR8IEZO1187B99055E',year='2008') LOCATION '/partitions_page_views' ;

!hdfs dfs -put /employee/songs.csv /partitions_page_views ;

LOAD DATA LOCAL INPATH '/employee/songs.csv' INTO TABLE songs partition(artist_name='AR8IEZO1187B99055E',year='2008');

create external table assign2.staging_songs(
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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/data/songs';


from staging_songs
insert overwrite table partitioned_songs partition (artist_name,year)
select * ;

from staging_songs
insert overwrite table songs partition(artist_name,year)
select artist_id , artist_latitude , artist_location , artist_longtitude , artist_name , duration , num_songs , song_id , title , year ;


create  table assign2.partitioned_songs_managed(
artist_id STRING,
artist_latitude DOUBLE ,
artist_location STRING,
artist_longtitude DOUBLE,
duration DOUBLE,
num_songs INT ,
song_id STRING,
title STRING)
partitioned by (artist_name STRING, year STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';


from staging_songs
insert overwrite table  partitioned_songs_managed partition (artist_name , year)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration , num_songs , song_id ,title , artist_name , year ;


truncate table partitioned_songs_managed ;


from staging_songs
insert overwrite table songs partition (year='2008' ,artist_name )
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration , num_songs , song_id ,title , artist_name
where year='2008'
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration , num_songs , song_id ,title , artist_name
where year='2003'
insert overwrite table songs partition (year='2004' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration,num_songs , song_id ,title , artist_name
where year='2004'
insert overwrite table songs partition (year='1994' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
 where year='1994'
 insert overwrite table songs partition (year='0' , artist_name)
 select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
where year='0'
insert overwrite table songs partition (year='2000' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
where year='2000'
insert overwrite table songs partition (year='2005' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
where year='2005'
insert overwrite table songs partition (year='1999' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
 where year='1999'
insert overwrite table songs partition (year='1993' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
where year='1993'
insert overwrite table songs partition (year='1985' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
where year='1985'
insert overwrite table songs partition (year='1964' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
 where year='1964'
insert overwrite table songs partition ( year='1992' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
 , num_songs , song_id ,title , artist_name
 where year='1992'
 insert overwrite table songs partition (year='1661' , artist_name)
 select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
where year='1661'
insert overwrite table songs partition (year='1972' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
where year='1972'
insert overwrite table songs partition (year='1997' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
where year='1997'
insert overwrite table songs partition (year='1986', artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
 where year='1986'
insert overwrite table songs partition (year='1987' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
 where year='1987'
insert overwrite table songs partition (year='1984',artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
where year='1984'
insert overwrite table songs partition (year='1982',artist_name)
 select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name
where year='1982'
insert overwrite table songs partition (year='1969',artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration
, num_songs , song_id ,title , artist_name where year='1969';


create table assign2.events_mg(
artist STRING,
auth STRING,
firstName STRING ,
gender STRING,
itemInSession STRING,
lastName STRING,
length DOUBLE ,
level STRING,
location STRING,
method STRING,
page STRING,
registeration STRING,
sessionId INT,
song STRING,
status INT,
ts STRING,
userAgent STRING,
userId INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';


LOAD DATA LOCAL INPATH '/employee/events.csv' INTO TABLE assign2.events_mg ;

select userId , sessionId , FIRST_VALUE(song)OVER(PARTITION BY sessionId ORDER BY ts
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_song ,
LAST_VALUE(song)OVER(PARTITION BY sessionId ORDER BY ts  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as Last_song
from events_mg ;



SELECT t1.userId , RANK()OVER(ORDER BY t1.count_distinct) as ranking
FROM
(SELECT userId , COUNT(DISTINCT song) as count_distinct
FROM events_mg
GROUP BY userId )t1 ;


SELECT t1.userId , ROW_NUMBER() OVER(ORDER BY t1.count_distinct) as ranking
FROM
(SELECT userId , COUNT(DISTINCT song) as count_distinct
FROM events_mg
WHERE page='NextSong'
GROUP BY userId)t1;


SELECT location , artist , COUNT(song) as count_songs
FROM events_mg
GROUP BY location , artist
GROUPING SETS ((location,artist),location,()) ;


SELECT location , artist , COUNT(song) as count_songs
FROM events_mg
GROUP BY location , artist
GROUPING SETS ((location,artist),location,artist,()) ;


SELECT userId , song ,
LEAD(song,1) OVER (PARTITION BY userId ORDER BY ts) as NEXT_SONG,
LAG(song,0) OVER (PARTITION BY userId ORDER BY ts) as PREV_SONG
FROM events_mg
WHERE page='NextSong' ;

SELECT userId , song , ts
FROM events_mg
ORDER BY userId , song ,ts ;


SELECT userId , song , ts
FROM events_mg
CLUSTER BY  userId , song ,ts ;




