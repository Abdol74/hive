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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES("skip.header.line.count"="1");


LOAD DATA LOCAL INPATH '/employee/events.csv' INTO TABLE assign2.events_mg ;

use assign2;

select userId , sessionId , FIRST_VALUE(song)OVER(PARTITION BY sessionId ORDER BY ts
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_song ,
LAST_VALUE(song)OVER(PARTITION BY sessionId ORDER BY ts  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as Last_song
from assign2.events_mg ;



SELECT t1.userId , RANK()OVER(ORDER BY t1.count_distinct) as ranking
FROM
(SELECT userId , COUNT(DISTINCT song) as count_distinct
FROM assign2.events_mg
GROUP BY userId )t1 ;


SELECT t1.userId , ROW_NUMBER() OVER(ORDER BY t1.count_distinct) as ranking
FROM
(SELECT userId , COUNT(DISTINCT song) as count_distinct
FROM events_mg
WHERE page='NextSong'
GROUP BY userId)t1;


SELECT location , artist , COUNT(song) as count_songs
FROM assign2.events_mg
GROUP BY location , artist
GROUPING SETS ((location,artist),location,()) ;


SELECT location , artist , COUNT(song) as count_songs
FROM assign2.events_mg
GROUP BY location , artist
GROUPING SETS ((location,artist),location,artist,()) ;


SELECT userId , song ,
LEAD(song,1) OVER (PARTITION BY userId ORDER BY ts) as NEXT_SONG,
LAG(song,0) OVER (PARTITION BY userId ORDER BY ts) as PREV_SONG
FROM assign2.events_mg
WHERE page='NextSong' ;

SELECT userId , song , ts
FROM assign2.events_mg
ORDER BY userId , song ,ts ;


SELECT userId , song , ts
FROM assign2.events_mg
CLUSTER BY  userId , song ,ts ;