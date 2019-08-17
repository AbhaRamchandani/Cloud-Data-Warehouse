import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# The SERIAL command in Postgres is not supported in Redshift.
# The equivalent in redshift is IDENTITY(0,1)
staging_events_table_create = ("""
CREATE TABLE staging_events(
    event_id INT IDENTITY(0,1) PRIMARY KEY,
    artist_name VARCHAR(255),
    auth VARCHAR(100),
    firstName VARCHAR(255),
    gender  VARCHAR(1),
    item_in_session    INTEGER,
    lastName VARCHAR(255),
    length	REAL,
    level VARCHAR(100),
    location VARCHAR(255),
    method VARCHAR(100),
    page VARCHAR(100),
    registration VARCHAR(100),
    session_id	BIGINT,
    song VARCHAR(255),
    status INTEGER,
    ts VARCHAR(100),
    user_agent TEXT,
    user_id TEXT);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id TEXT PRIMARY KEY,
    num_songs INTEGER,
    artist_id TEXT,
    artist_latitude REAL,
    artist_longitude REAL,
    artist_location TEXT,
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration REAL,
    year INTEGER);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP REFERENCES time(start_time),
    user_id TEXT REFERENCES users(user_id),
    level VARCHAR(100),
    song_id TEXT REFERENCES song(song_id),
    artist_id TEXT REFERENCES artist(artist_id),
    session_id BIGINT,
    location VARCHAR(255),
    user_agent TEXT);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id TEXT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(1),
    level VARCHAR(100));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song(
    song_id TEXT PRIMARY KEY,
    title VARCHAR(255),
    artist_id TEXT NOT NULL,
    year INTEGER,
    duration REAL);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist(
    artist_id TEXT PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude REAL,
    longitude REAL);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER);
""")

# Load Staging/Fact Tables
staging_events_copy = ("COPY staging_events FROM {} \
                      iam_role {} \
                      FORMAT AS JSON {};".format(config.get("S3", "LOG_DATA"),
                                                 config.get("IAM_ROLE", "ARN"),
                                                 config.get("S3", "LOG_JSONPATH")))

staging_songs_copy = ("COPY staging_songs FROM {} \
                     iam_role {} \
                     FORMAT AS JSON 'auto';".format(config.get("S3", "SONG_DATA"),
                                                    config.get("IAM_ROLE", "ARN")))

# Load Dimension Tables
# Used https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
# to get the start_time value for the table
songplay_table_insert = ("""
INSERT INTO songplay
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
FROM staging_events se, staging_songs ss
WHERE se.song = ss.title;
""")

user_table_insert = ("""
INSERT INTO users
(user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    user_Id,
    firstName,
    lastName,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong';
""")

artist_table_insert = ("""
INSERT INTO artist
(artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs;
""")

song_table_insert = ("""
INSERT INTO song
(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, week, month, year, weekday)
SELECT
    start_time,
    EXTRACT(hr from start_time) as hour,
    EXTRACT(d from start_time) as day,
    EXTRACT(w from start_time) as week,
    EXTRACT(mon from start_time) as month,
    EXTRACT(yr from start_time) as year,
    EXTRACT(weekday from start_time) AS weekday
FROM (SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time
      FROM staging_events s);
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_create, artist_table_create,
                        song_table_create, time_table_create,
                        songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert,
                        song_table_insert, time_table_insert,
                        songplay_table_insert]
