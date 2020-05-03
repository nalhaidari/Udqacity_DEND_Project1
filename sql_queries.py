# DROP TABLES

songplay_table_drop = "Drop table if exists songplays ;"
user_table_drop = "Drop table if exists users ;"
song_table_drop = "Drop table if exists songs ;"
artist_table_drop = "Drop table if exists artists ;"
time_table_drop = "Drop table if exists time ;"

# CREATE TABLES

songplay_table_create = ("""
Create table if not exists songplays (songplay_id SERIAL PRIMARY KEY, start_time TIMESTAMP NOT NULL, user_id int NOT NULL, level VARCHAR, song_id VARCHAR, 
artist_id VARCHAR , session_ID int, location VARCHAR, user_agent VARCHAR)


""")

user_table_create = (""" Create table if not exists users(userId  int PRIMARY KEY Not NULL,firstName VARCHAR,lastName VARCHAR, gender VARCHAR,level VARCHAR)
""")

song_table_create = (""" Create table if not exists songs (song_id VARCHAR PRIMARY KEY Not NULL , title VARCHAR,  artist_id VARCHAR,year int ,duration numeric)
""")

artist_table_create = (""" Create table if not exists artists ( artist_id VARCHAR PRIMARY KEY Not NULL , artist_latitude numeric, artist_location VARCHAR ,artist_longitude numeric, artist_name VARCHAR)
""")

time_table_create = (""" Create table if not exists time (start_time bigint primary key not null,time_stamp timestamp,hour int, day int, week_of_year int, month text, year int, weekday VARCHAR)
""")

# INSERT RECORDS

songplay_table_insert = (""" Insert into songplays (start_time, user_id, level , song_id,artist_id , session_ID , location , user_agent ) 
values (%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = (""" INSERT INTO users (userId ,firstName ,lastName , gender ,level) values (%s,%s,%s,%s,%s)
ON CONFLICT (userId) DO 
UPDATE 
SET level = EXCLUDED.level """)

song_table_insert = ("""Insert into songs (song_id, title, artist_id , year,duration) values (%s,%s,%s,%s,%s)
ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
Insert into artists (artist_id, artist_latitude, artist_location, artist_longitude, artist_name) values (%s,%s,%s,%s,%s)
ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""
Insert into time (start_time, time_stamp,hour,day,week_of_year,month,year,weekday) values (%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS

song_select = (""" with s as(select song_id from songs where title = %s and duration = %s limit 1)
,a as (select artist_id from artists where  artist_name = %s limit 1)
select s.song_id , a.artist_id from s cross join a;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]