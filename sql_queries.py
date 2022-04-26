import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stagingEvents"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagingSongs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE stagingEvents (
    artist VARCHAR(30) NOT NULL,
    auth VARCHAR(20) NOT NULL,
    firstName VARCHAR(30) NOT NULL,
    lastName VARCHAR(30) NOT NULL,
    gender VARCHAR(1) NOT NULL,
    itemInSession SMALLINT NOT NULL,
    length NUMERIC NOT NULL,
    level VARCHAR (15) NOT NULL,
    location VARCHAR(50) NOT NULL,
    method VARCHAR(10) NOT NULL,
    page VARCHAR(20) NOT NULL,
    registration NUMERIC NOT NULL,
    sessionId INTEGER NOT NULL,
    song VARCHAR(40) NOT NULL,
    status SMALLINT NOT NULL,
    ts BIGINT NOT NULL,
    userAgent VARCHAR(100) NOT NULL,
    userId INTEGER NOT NULL
);
"""

staging_songs_table_create = """
CREATE TABLE stagingSongs (
    artist_id VARCHAR(20) NOT NULL,
    artist_latitude NUMERIC NOT NULL,
    artist_location VARCHAR(30) NOT NULL,
    artist_longitude  NUMERIC NOT NULL,
    artist_name VARCHAR(30) NOT NULL,
    duration NUMERIC NOT NULL,
    num_songs SMALLINT NOT NULL,
    song_id VARCHAR(20) NOT NULL,
    title VARCHAR(80) NOT NULL,
    year SMALLINT NOT NULL
);
"""

songplay_table_create = """
CREATE TABLE songplays (
    songplay_id INTEGER NOT NULL, 
    start_time TIMESTAMP NOT NULL, 
    user_id INTEGER NOT NULL, 
    level VARCHAR(10) NOT NULL, 
    song_id VARCHAR(20), 
    artist_id VARCHAR(20), 
    session_id INTEGER NOT NULL, 
    location VARCHAR(50) NOT NULL,
    user_agent VARCHAR(100) NOT NULL
);
"""

user_table_create = """
CREATE TABLE users (
    user_id INTEGER NOT NULL, 
    first_name VARCHAR(30) NOT NULL,
    last_name VARCHAR(30) NOT NULL,
    gender VARCHAR(1) NOT NULL, 
    level VARCHAR(10) NOT NULL
);
"""

song_table_create = """
CREATE TABLE songs (
    song_id INTEGER NOT NULL, 
    title VARCHAR(80) NOT NULL, 
    artist_id VARCHAR(20) NOT NULL, 
    year SMALLINT NOT NULL, 
    Duration NUMERIC NOT NULL
);
"""

artist_table_create = """
CREATE TABLE artists (
    artist_id VARCHAR(20) NOT NULL,
    name VARCHAR(30) NOT NULL,
    location VARCHAR(30) NOT NULL,
    latitude NUMERIC NOT NULL,
    longitude  NUMERIC NOT NULL,  
);
"""

time_table_create = """
CREATE TABLE artists (
    start_time TIMESTAMP NOT NULL, 
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL, 
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL,
)
"""

# STAGING TABLES

staging_events_copy = (
    """
"""
).format()

staging_songs_copy = (
    """
"""
).format()

# FINAL TABLES

songplay_table_insert = """
"""

user_table_insert = """
"""

song_table_insert = """
"""

artist_table_insert = """
"""

time_table_insert = """
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
