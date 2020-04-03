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
# STAGING TABLES
staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    event_key BIGINT IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId BIGINT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId BIGINT);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    song_key BIGINT IDENTITY(0,1) PRIMARY KEY,
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT);
"""

# FINAL TABLES
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_key BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time BIGINT NOT NULL,
    user_key BIGINT,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id BIGINT,
    location VARCHAR,
    user_agent VARCHAR);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_key BIGINT IDENTITY(0,1) PRIMARY KEY,
    user_id BIGINT,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR NOT NULL,
    year INT,
    duration FLOAT);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time BIGINT PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT);
""")

# STAGING TABLES
staging_events_copy = f"""
COPY staging_events
FROM {config['S3']['LOG_DATA']}
REGION 'us-west-2'
IAM_ROLE {config['IAM_ROLE']['ARN']}
FORMAT AS JSON {config['S3']['LOG_JSONPATH']};

DELETE FROM staging_events
WHERE page != 'NextSong';

ALTER TABLE staging_events
ADD COLUMN timestamp TIMESTAMP;

UPDATE staging_events
SET timestamp = TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'
"""

staging_songs_copy = f"""
COPY staging_songs
FROM {config['S3']['SONG_DATA']}
REGION 'us-west-2'
IAM_ROLE {config['IAM_ROLE']['ARN']}
FORMAT AS JSON 'auto'
"""

staging_add_timestamp_to_events = """
ALTER TABLE staging_events
ADD COLUMN timestamp TIMESTAMP
"""

staging_update_timestamp_to_events = """
UPDATE staging_events
SET timestamp = TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second'
"""

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_key, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    t.start_time,
    u.user_key,
    u.level,
    s.song_id,
    a.artist_id,
    e.sessionId,
    a.location,
    e.userAgent
FROM staging_events e
INNER JOIN 
    time t ON (TO_CHAR(e.timestamp :: TIMESTAMP, 'yyyyMMDDHH24')::integer = t.start_time)
INNER JOIN
    users u ON (e.userId = u.user_id AND e.firstname = u.first_name AND e.lastname = u.last_name AND e.level = u.level)
INNER JOIN
    songs s ON (e.song = s.title)
INNER JOIN
    artists a ON (e.artist = a.name)
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    userId,
    firstName,
    lastName,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
    DISTINCT(TO_CHAR(timestamp :: TIMESTAMP, 'yyyyMMDDHH24')::integer) as start_time,
    EXTRACT(HOUR FROM timestamp) as hour,
    EXTRACT(DAY FROM timestamp) as day,
    EXTRACT(WEEK FROM timestamp) as week,
    EXTRACT(MONTH FROM timestamp) as month,
    EXTRACT(YEAR FROM timestamp) as year,
    EXTRACT(WEEKDAY FROM timestamp) as weekday 
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create
                       ]
drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop
                     ]
copy_table_queries = [staging_events_copy,
                      staging_songs_copy
                     ]
insert_table_queries = [user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert,
                        songplay_table_insert
                       ]
