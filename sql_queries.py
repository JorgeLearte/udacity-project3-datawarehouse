import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""create table IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar not null,
        firstName varchar,
        gender char (1),
        itemInSession int not null,
        lastName varchar,
        length numeric,
        level varchar not null,
        location varchar,
        method varchar not null,
        page varchar not null,
        registration numeric,
        sessionId int not null,
        song varchar,
        status int not null,
        ts numeric not null,
        userAgent varchar,
        userId int
)""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (song_id varchar PRIMARY KEY, num_songs int, artist_id varchar, artist_latitude varchar, artist_longitude varchar, artist_location varchar, artist_name varchar, title varchar, duration double precision, year int)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id INT IDENTITY(1, 1) PRIMARY KEY, start_time varchar NOT NULL, user_id varchar NOT NULL, level varchar, song_id varchar, artist_id varchar, session_id varchar, location varchar, user_agent varchar)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration float)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name varchar, location varchar, latitude varchar, longitude varchar)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time varchar PRIMARY KEY, hour int, day int, week int, month int, year int, weekday varchar)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    json 's3://udacity-dend/log_json_path.json'
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data'
credentials 'aws_iam_role={}'
json 'auto'""").format(DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT timestamp 'epoch' + e.ts / 1000 * interval '1 second' as start_time, e.userId as user_id, e.level, s.song_id,
s.artist_id, e.sessionId as session_id, e.location, e.userAgent as user_agent
FROM STAGING_EVENTS e LEFT JOIN staging_songs s ON e.song = s.title and e.artist = s.artist_name where e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT userId, firstName, lastName, gender, level FROM staging_events e
JOIN (
        SELECT MAX(ts) as ts, userId
        FROM staging_events
        WHERE page = 'NextSong'
        GROUP BY userId
    ) et on e.userId = et.userId and e.ts = et.ts
""")

user_table_insert = ("""
    insert into users
    select eo.userId, eo.firstName, eo.lastName, eo.gender, eo.level
    from staging_events eo
    join (
        select max(ts) as ts, userId
        from staging_events
        where page = 'NextSong'
        group by userId
    ) ei on eo.userId = ei.userId and eo.ts = ei.ts
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title , artist_id, year, duration)
SELECT song_id, title , artist_id, year, duration FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs
""")

time_table_insert = ("""
    insert into time
    select
        staging_time.start_time,
        extract(hour from staging_time.start_time) as hour,
        extract(day from staging_time.start_time) as day,
        extract(week from staging_time.start_time) as week,
        extract(month from staging_time.start_time) as month,
        extract(year from staging_time.start_time) as year,
        extract(weekday from staging_time.start_time) as weekday
    from (
        select distinct timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time
        from staging_events
        where page = 'NextSong'
    ) staging_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]