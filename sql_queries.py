import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

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
CREATE TABLE IF NOT EXISTS staging_events(
    artist TEXT, 
    auth TEXT, 
    first_name TEXT, 
    gender TEXT, 
    item_in_session INTEGER,
    last_name TEXT, 
    length NUMERIC, 
    level TEXT, 
    location TEXT, 
    method TEXT, 
    page TEXT, 
    registration NUMERIC, 
    session_id INTEGER, 
    song TEXT, 
    status INTEGER, 
    ts BIGINT, 
    user_agent TEXT, 
    user_id INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INTEGER NOT NULL, 
    artist_id TEXT,
    artist_location TEXT,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_name TEXT,
    song_id TEXT,
    title TEXT,
    duration NUMERIC,
    year INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP sortkey,
    user_id INTEGER NOT NULL distkey,
    level TEXT NOT NULL,
    song_id TEXT,
    artist_id TEXT,
    session_id INTEGER,
    location TEXT,
    user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    level TEXT sortkey
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id TEXT NOT NULL PRIMARY KEY,
    title TEXT,
    artist_id TEXT NOT NULL sortkey,
    year INTEGER NOT NULL,
    duration NUMERIC NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id TEXT NOT NULL PRIMARY KEY sortkey,
    name TEXT distkey,
    location TEXT,
    latitude NUMERIC,
    longitude NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP NOT NULL PRIMARY KEY sortkey,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
);
""")

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
    FROM staging_events se
    JOIN staging_songs ss
    ON se.artist = ss.artist_name AND se.length = ss.duration AND se.song = ss.title
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT start_time,
           EXTRACT(hour FROM start_time),
           EXTRACT(day FROM start_time),
           EXTRACT(week FROM start_time),
           EXTRACT(month FROM start_time),
           EXTRACT(year FROM start_time),
           EXTRACT(dow FROM start_time)
           FROM songplays;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, 
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, 
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
