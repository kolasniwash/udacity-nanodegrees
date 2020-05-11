import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
artist varchar,
auth varchar,
first_name varchar,
gender varchar,
item_in_session int,
last_name varchar,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration varchar,
session_id int,
song varchar,
status int,
ts varchar,
user_agent varchar,
user_id int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
num_songs int,
artist_id varchar,
artist_latitude float,
artist_longitude float,
artist_location varchar, 
artist_name varchar, 
song_id varchar, 
title varchar, 
duration float, 
year int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id INT IDENTITY(0,1),
start_time TIMESTAMP NOT NULL, 
user_id int, 
level varchar,
song_id varchar, 
artist_id varchar,
session_id int,
location varchar,
user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar PRIMARY KEY,
title varchar,
artist_id varchar,
year int,
duration float
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar PRIMARY KEY,
name varchar,
location varchar,
latitude float,
longitude float
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY,
hour int,
day int,
week int,
month int,
year int,
weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM '{}'
CREDENTIALS 'aws_iam_role={}'
JSON {} REGION 'us-west-2';

""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'), 
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM '{}' 
CREDENTIALS 'aws_iam_role={}'
JSON 'auto' REGION 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), 
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
## FOr exac table we select distinct events. No duplicates per table.

## songplays join with staging_songs to get song_ id and artist_id.
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time,
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
FROM staging_events as e, staging_songs as s
WHERE e.song=s.title AND e.artist = s.artist_name 
AND e.length=s.duration AND e.page='NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM staging_events
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id, 
    title,
    artist_id,
    year,
    duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as lonitude
FROM staging_songs;
""")

## Cast ts to a timestamp value and extract date parts.
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    st.start_time,
    EXTRACT(hour from st.start_time) as hour,
    EXTRACT(day from st.start_time) as day,
    EXTRACT(week from st.start_time) as week,
    EXTRACT(month from st.start_time)as month,
    EXTRACT(year from st.start_time) as year,
    EXTRACT(WEEKDAY from st.start_time) as weekday
FROM ( 
    SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time
    FROM staging_events
    ) as st;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
