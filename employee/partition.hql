use assign2;
from staging_songs 
insert overwrite table partitioned_songs_managed partition (year='2008' ,artist_name )
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration , num_songs , song_id ,title , artist_name
where year='2008'
insert overwrite table partitioned_songs_managed partition (year='2003' , artist_name )
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration , num_songs , song_id ,title , artist_name
where year='2003'
insert overwrite table partitioned_songs_managed partition (year='2004' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='2004'
insert overwrite table partitioned_songs_managed partition (year='1994' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1994'
insert overwrite table partitioned_songs_managed partition (year='0' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='0'
insert overwrite table partitioned_songs_managed partition (year='2000' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='2000'
insert overwrite table partitioned_songs_managed partition (year='2005' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='2005'
insert overwrite table partitioned_songs_managed partition (year='1999' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1999'
insert overwrite table partitioned_songs_managed partition (year='1993' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1993'
insert overwrite table partitioned_songs_managed partition (year='1985' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1985'
insert overwrite table partitioned_songs_managed partition (year='1964' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1964'
insert overwrite table partitioned_songs_managed partition ( year='1992' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1992'
insert overwrite table partitioned_songs_managed partition (year='1661' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1661'
insert overwrite table partitioned_songs_managed partition (year='1972' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1972'
insert overwrite table partitioned_songs_managed partition (year='1997' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1997'
insert overwrite table partitioned_songs_managed partition (year='1986', artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1986'
insert overwrite table partitioned_songs_managed partition (year='1987' , artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1987'
insert overwrite table partitioned_songs_managed partition (year='1984',artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1984'
insert overwrite table partitioned_songs_managed partition (year='1982',artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1982'
insert overwrite table partitioned_songs_managed partition (year='1969',artist_name)
select artist_id , artist_latitude , artist_location , artist_longtitude  , duration 
, num_songs , song_id ,title , artist_name
where year='1969'	;


