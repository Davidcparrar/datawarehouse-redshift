import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

AWS_REGION = config.get("AWS", "AWS_REGION")
IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stagingEvents"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagingSongs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE stagingEvents (
    artist VARCHAR(200),
    auth VARCHAR(20) NOT NULL,
    first_name VARCHAR(30),
    gender VARCHAR(1),
    item_in_session SMALLINT NOT NULL,
    last_name VARCHAR(30),   
    length NUMERIC,
    level VARCHAR(10),
    location VARCHAR(50),
    method VARCHAR(10) NOT NULL,
    page VARCHAR(20) NOT NULL,
    registration NUMERIC,
    session_id INTEGER NOT NULL,
    song TEXT,
    status SMALLINT NOT NULL,
    ts BIGINT NOT NULL,
    user_agent TEXT,
    user_id INTEGER
);
"""

staging_songs_table_create = """
CREATE TABLE stagingSongs (
    song_id VARCHAR(20) NOT NULL,
    num_songs SMALLINT NOT NULL,
    title VARCHAR(300) NOT NULL,
    artist_name VARCHAR(400) NOT NULL,
    artist_latitude NUMERIC,
    year SMALLINT NOT NULL,
    duration NUMERIC NOT NULL,
    artist_id VARCHAR(20) NOT NULL,
    artist_longitude NUMERIC,
    artist_location VARCHAR(300) NOT NULL
);
"""

songplay_table_create = """
CREATE TABLE songplays (
    songplay_id INTEGER NOT NULL identity(0, 1), 
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id INTEGER,
    level VARCHAR(10),
    song_id VARCHAR(20) DISTKEY,
    artist_id VARCHAR(20),
    session_id INTEGER,
    location VARCHAR(300),
    user_agent TEXT
);
"""


user_table_create = """
CREATE TABLE users (
    user_id INTEGER NOT NULL SORTKEY, 
    first_name VARCHAR(30),
    last_name VARCHAR(30),
    gender VARCHAR(1), 
    level VARCHAR(10)
)
DISTSTYLE ALL;
"""

song_table_create = """
CREATE TABLE songs (
    song_id VARCHAR(20) NOT NULL SORTKEY DISTKEY, 
    title VARCHAR(300) NOT NULL, 
    artist_id VARCHAR(20) NOT NULL, 
    year SMALLINT, 
    duration NUMERIC
);
"""

artist_table_create = """
CREATE TABLE artists (
    artist_id VARCHAR(20) NOT NULL SORTKEY,
    name VARCHAR(400) NOT NULL,
    location VARCHAR(300),
    latitude NUMERIC,
    longitude NUMERIC  
);
"""

time_table_create = """
CREATE TABLE time (
    start_time TIMESTAMP NOT NULL SORTKEY, 
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL, 
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
)
DISTSTYLE ALL;
"""

# STAGING TABLES

staging_events_copy = (
    """copy stagingEvents from {}
iam_role {}
region '{}'
json {};
"""
).format(LOG_DATA, IAM_ROLE_ARN, AWS_REGION, LOG_JSONPATH)

staging_songs_copy = (
    """copy stagingSongs from {}
iam_role {}
region '{}'
json 'auto';
    """
).format(SONG_DATA, IAM_ROLE_ARN, AWS_REGION)

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (
    start_time, 
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT date_add('ms',se.ts,'1970-01-01') as start_time,
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
FROM stagingEvents se
JOIN stagingSongs ss
ON se.artist = ss.artist_name AND  se.song = ss.title;
"""

user_table_insert = """
INSERT INTO users (user_id, 
    first_name,
    last_name,
    gender, 
    level
)
SELECT DISTINCT(user_id),
	first_name,
    last_name,
    gender,
    level
FROM stagingEvents
WHERE user_id IS NOT NULL;
"""

song_table_insert = """
INSERT INTO songs (
    song_id,
    title, 
    artist_id, 
    year, 
    duration
)
SELECT DISTINCT(song_id),
    title, 
    artist_id, 
    year, 
    duration
FROM stagingSongs
WHERE song_id IS NOT NULL;
"""

artist_table_insert = """
INSERT INTO artists (
    artist_id,
    name, 
    location, 
    latitude, 
    longitude
)
SELECT DISTINCT(artist_id),
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM stagingSongs
WHERE artist_id IS NOT NULL;
"""

time_table_insert = """
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
) 
SELECT DISTINCT(date_add('ms',ts,'1970-01-01')) as start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week, 
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(weekday FROM start_time) AS weekday
FROM stagingEvents;
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
