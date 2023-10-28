from config import Config
from utils.helpers import drop_table


# CONFIG
cfg = Config()
cfg.read()

# DROP TABLES
staging_events_table_drop = drop_table('staging_events')
staging_songs_table_drop = drop_table('staging_songs')
songplays_table_drop = drop_table('songplays')
users_table_drop = drop_table('users')
songs_table_drop = drop_table('songs')
artists_table_drop = drop_table('artists')
time_table_drop = drop_table('time')

# CREATE TABLES
staging_events_table_create= """
create table if not exists staging_events (
    artist varchar(252),
    auth varchar(10) not null,
    firstName varchar(32),
    gender varchar(1),
    itemInSession smallint not null,
    lastName varchar(64),
    length float,
    level varchar(4) not null,
    location varchar(64),
    method varchar(8) not null,
    page varchar(32) not null,
    registration float,
    sessionId smallint not null,
    song varchar(256),
    status smallint not null,
    ts bigint not null,
    userAgent varchar(256),
    userId varchar(10)
);
"""

staging_songs_table_create = """
create table if not exists staging_songs (
    artist_id varchar(18) not null,
    artist_latitude float,
    artist_location varchar(252),
    artist_longitude float,
    artist_name varchar(252) not null,
    duration float not null,
    num_songs smallint not null,
    song_id varchar(18) not null,
    title varchar(256) not null,
    year smallint not null,
    primary key (song_id)
);
"""

songplays_table_create = """
create table if not exists songplays (
    songplay_id integer identity(0,1) primary key,
    start_time timestamp,
    user_id integer,
    level varchar(4),
    song_id varchar(18),
    artist_id varchar(18),
    session_id smallint,
    location varchar(64),
    user_agent varchar(256)
);
"""

users_table_create = """
create table if not exists users (
	user_id integer not null,
    first_name varchar(32),
    last_name varchar(64),
    gender varchar(1),
    level varchar(4) not null,
    primary key (user_id, level)
);
"""

songs_table_create = """
create table if not exists songs (
    song_id varchar(18) primary key,
    title varchar(256) not null,
    artist_id varchar(18) not null,
    year smallint not null,
    duration float not null
);
"""

artists_table_create = """
create table if not exists artists (
    artist_id varchar(18) primary key,
    name varchar(252) not null,
    location varchar(252),
    latitude float,
    longitude float
);
"""

time_table_create = """
create table if not exists time (
    start_time timestamp primary key,
    hour smallint not null,
    day smallint not null,
    week smallint not null,
    month smallint not null,
    year smallint not null,
    weekday smallint not null
);
"""

# STAGING TABLES
staging_events_copy = f"""
copy staging_events
from '{cfg.s3_log_data}'
credentials 'aws_iam_role={cfg.iam_role_arn}'
region 'us-west-2'
json '{cfg.s3_log_jsonpath}';
"""

staging_songs_copy = f"""
copy staging_songs
from '{cfg.s3_song_data}'
credentials 'aws_iam_role={cfg.iam_role_arn}'
region 'us-west-2'
format as json 'auto';
"""

# FINAL TABLES
songplays_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplays_table_drop,
    users_table_drop,
    songs_table_drop,
    artists_table_drop,
    time_table_drop]

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplays_table_create,
    users_table_create,
    songs_table_create,
    artists_table_create,
    time_table_create]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy]

insert_table_queries = [
    songplays_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert]
