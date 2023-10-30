from config import Config
from utils.helpers import drop_table, count_number_of_rows_query


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
songplays_table_insert = """
insert into songplays (
    start_time, user_id, level, song_id,
    artist_id, session_id, location, user_agent)
select
    timestamp 'epoch' + (e.ts / 1000) * interval '1 second' as start_time,
    cast(e.userId as integer) as user_id,
    e.level as level,
    s.song_id as song_id,
    s.artist_id as artist_id,
    e.sessionId as session_id,
    e.location as location,
    e.userAgent as user_agent
from staging_events as e
left join staging_songs as s
on  (
    e.song = s.title and
    e.artist = s.artist_name and
    e.length = s.duration)
where e.page = 'NextSong';
"""

users_table_insert = """
insert into users
select
    cast(userid as integer) user_id,
    firstname first_name,
    lastname last_name,
    gender,
    level
from staging_events
where page = 'NextSong'
group by user_id, first_name, last_name, gender, level;
"""

songs_table_insert = """
insert into songs
select
    song_id,
    title,
    artist_id,
    year,
    duration
from staging_songs;
"""

artists_table_insert = """
insert into artists
select
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
from staging_songs
group by artist_id, name, location, latitude, longitude;
"""

time_table_insert = """
insert into time
with
unique_ts as (
    select
        distinct timestamp 'epoch' + (ts / 1000) * interval '1 second'
            as start_time
    from staging_events
    where page = 'NextSong')
select
    start_time,
    extract(hour from start_time) "hour",
    extract(day from start_time) "day",
    extract(week from start_time) "week",
    extract(month from start_time) "month",
    extract(year from start_time) "year",
    extract(weekday from start_time) "weekday"
from unique_ts;
"""

# EXAMPLE OF QUERIES
# ROW COUNT QUERIES
row_count_headers = ['number_of_rows']
staging_events_row_count = count_number_of_rows_query('staging_events')
staging_songs_row_count = count_number_of_rows_query('staging_songs')
songplays_row_count = count_number_of_rows_query('songplays')
users_row_count = count_number_of_rows_query('users')
songs_row_count = count_number_of_rows_query('songs')
artists_row_count = count_number_of_rows_query('artists')
time_row_count = count_number_of_rows_query('time')
# USERS QUERIES
users_by_gender_question = 'What is the users distribution by gender?'
users_by_gender_headers = ['gender', 'number_of_users']
users_by_gender = """
select
    gender,
    count(1) as number_of_users
from users
group by gender;
"""
users_by_level_question = 'What is the users distribution by level (free/paid)?'
users_by_level_headers = ['level', 'number_of_users']
users_by_level = """
select
    level,
    count(1) as number_of_users
from users
group by level;
"""
# SONGPLAYS QUERIES
songplays_by_level_question = 'What is the number of plays distribution by level?'
songplays_by_level_headers = ['level', 'number_of_plays']
songplays_by_level = """
select
    level,
    count(1) as number_of_plays
from songplays
group by level;
"""
songplays_by_hour_question = 'What is the number of plays distribution by hour?'
songplays_by_hour_headers = ['hour', 'number_of_plays']
songplays_by_hour = """
select
    t.hour,
    count(1) as number_of_plays
from songplays as s
join time as t
on s.start_time = t.start_time
group by t.hour
order by t.hour;
"""


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
    users_table_insert,
    songs_table_insert,
    artists_table_insert,
    time_table_insert]

row_count_queries = [
    (row_count_headers, staging_events_row_count),
    (row_count_headers, staging_songs_row_count),
    (row_count_headers, songplays_row_count),
    (row_count_headers, users_row_count),
    (row_count_headers, songs_row_count),
    (row_count_headers, artists_row_count),
    (row_count_headers, time_row_count)]

users_queries = [
    (users_by_gender_question, users_by_gender_headers, users_by_gender),
    (users_by_level_question, users_by_level_headers, users_by_level),
]

songplays_queries = [
    (songplays_by_level_question, songplays_by_level_headers, songplays_by_level),
    (songplays_by_hour_question, songplays_by_hour_headers, songplays_by_hour),
]
