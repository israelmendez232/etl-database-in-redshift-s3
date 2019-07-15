# This script creates the necessary tables and their characteristics.
import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE staging_events(
        artist          VARCHAR(255),
        auth            VARCHAR(255),
        firstname       VARCHAR(255),
        gender          CHAR(1),
        iteminsession   INT,
        lastname        VARCHAR(255),    
        length          DOUBLE PRECISION,
        level           VARCHAR(255),
        location        VARCHAR(255),
        method          VARCHAR(255),
        page            VARCHAR(255),
        registration    BIGINT,
        sessionid       INT,
        song            VARCHAR(255),
        status          INT,
        ts              BIGINT,
        useragent       VARCHAR(255),        
        userid          INT        
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        artist_id           VARCHAR(255),
        artist_latitude     DOUBLE PRECISION,
        artist_location     VARCHAR(255),
        artist_longitude    DOUBLE PRECISION,
        artist_name         VARCHAR(255),
        duration            DOUBLE PRECISION,
        num_songs           INT,
        song_id             VARCHAR(255),
        title               VARCHAR(255),
        year                INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id     INT IDENTITY(0,1)  PRIMARY KEY,
        start_time      timestamp,
        user_id         INT,
        level           VARCHAR(255),
        song_id         VARCHAR(255),
        artist_id       VARCHAR(255),
        session_id      VARCHAR(255),
        location        VARCHAR(255),
        user_agent      VARCHAR(255)            
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id      INT              NOT NULL    PRIMARY KEY,
        first_name   VARCHAR(255),
        last_name    VARCHAR(255),
        gender       CHAR(1),                 
        level        VARCHAR(255)            
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id     VARCHAR(255)  NOT NULL    PRIMARY KEY,
        title       VARCHAR(255),
        artist_id   VARCHAR(255),
        year        INT,
        duration    DOUBLE PRECISION     
    );
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id   VARCHAR(255)  NOT NULL   PRIMARY KEY,
        name        VARCHAR(255),
        location    VARCHAR(255),
        latitude    DOUBLE PRECISION,
        longitude   DOUBLE PRECISION               
    );
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time  timestamp  NOT NULL  PRIMARY KEY,
        hour        INT,       
        day         INT,
        week        INT,
        month       INT,
        year        INT,
        weekday     INT
    );
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY {} 
    FROM 's3://udacity-dend/log_data' 
    IAM_ROLE {} 
    REGION 'us-west-2'
    FORMAT AS JSON {}
    TIMEFORMAT as 'epochmillisecs';
""").format('staging_events', config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY {}
    FROM 's3://udacity-dend/song_data' 
    IAM_ROLE {}
    JSON 'auto';
""").format('staging_songs', config['IAM_ROLE']['ARN'])

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        timestamp 'epoch' + e.ts * interval '1 second' AS start_time,
        e.userid as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionid AS session_id,
        e.location,
        e.useragent AS user_agent
    FROM staging_events e
    JOIN staging_songs s ON e.artist = s.artist_name AND e.song = s.title AND e.length = s.duration
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userid AS user_id,
        firstname AS first_name,
        lastname AS last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong';
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
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT
        start_time,
        EXTRACT (HOUR FROM start_time) AS hour,
        EXTRACT (DAY FROM start_time) AS day,
        EXTRACT (WEEK FROM start_time) AS week,
        EXTRACT (MONTH FROM start_time) AS month,
        EXTRACT (YEAR FROM start_time) AS year,
        EXTRACT (DOW FROM start_time) AS weekday
    FROM songplays;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
