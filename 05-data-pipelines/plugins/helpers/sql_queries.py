class SqlQueries:
    staging_copy = ("""
        TRUNCATE TABLE {table};
        COPY {table}
        FROM '{s3path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        FORMAT AS JSON '{jsonformat}'
    """)

    songplay_table_insert = ("""
        INSERT INTO {table} (
            playid,
            start_time,
            userid,
            "level",
            songid,
            artistid,
            sessionid,
            location,
            user_agent)
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time,
            events.userid,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.sessionid,
            events.location,
            events.useragent
            FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
        FROM staging_events
        WHERE
            page='NextSong'
            AND userid IS NOT NULL) events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    user_table_insert = ("""
        INSERT INTO {table} (
            userid,
            first_name,
            last_name,
            gender,
            "level"
        )
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE
            page='NextSong'
            AND userid IS NOT NULL
    """)

    song_table_insert = ("""
        INSERT INTO {table} (
            songid,
            title,
            artistid,
            "year",
            duration
        )
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO {table} (
            artistid,
            name,
            location,
            lattitude,
            longitude
        )
        SELECT
            distinct artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO {table} (
            start_time,
            "hour",
            "day",
            week,
            "month",
            "year",
            weekday
        )
        SELECT
            start_time,
            extract(hour from start_time),
            extract(day from start_time),
            extract(week from start_time),
            extract(month from start_time),
            extract(year from start_time),
            extract(dayofweek from start_time)
        FROM songplays
    """)

    row_count = ("""
        SELECT COUNT(*)
        FROM {table}
    """)
