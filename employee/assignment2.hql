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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES("skip.header.line.count"="1");


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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES("skip.header.line.count"="1");

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
TBLPROPERTIES("skip.header.line.count"="1")
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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES("skip.header.line.count"="1");


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