class SqlQueries:
    """
    All SQL statements used by the Sparkify ETL pipeline.
    Organized into: CREATE tables, COPY staging, INSERT transforms, and QUALITY checks.
    """

    # ──────────────────────────────────────────────
    # CREATE TABLE statements
    # ──────────────────────────────────────────────

    staging_events_table_create = """
        CREATE TABLE IF NOT EXISTS staging_events (
            artist          VARCHAR(256),
            auth            VARCHAR(256),
            firstName       VARCHAR(256),
            gender          CHAR(1),
            itemInSession   INTEGER,
            lastName        VARCHAR(256),
            length          NUMERIC(10,5),
            level           VARCHAR(256),
            location        VARCHAR(512),
            method          VARCHAR(10),
            page            VARCHAR(256),
            registration    BIGINT,
            sessionId       INTEGER,
            song            VARCHAR(256),
            status          INTEGER,
            ts              BIGINT,
            userAgent       VARCHAR(512),
            userId          INTEGER
        );
    """

    staging_songs_table_create = """
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs        INTEGER,
            artist_id        VARCHAR(256),
            artist_latitude  NUMERIC(9,6),
            artist_longitude NUMERIC(9,6),
            artist_location  VARCHAR(512),
            artist_name      VARCHAR(256),
            song_id          VARCHAR(256),
            title            VARCHAR(256),
            duration         NUMERIC(10,5),
            year             INTEGER
        );
    """

    songplay_table_create = """
        CREATE TABLE IF NOT EXISTS songplays (
            playid      VARCHAR(32) NOT NULL,
            start_time  TIMESTAMP   NOT NULL,
            userid      INTEGER     NOT NULL,
            level       VARCHAR(256),
            songid      VARCHAR(256),
            artistid    VARCHAR(256),
            sessionid   INTEGER,
            location    VARCHAR(512),
            user_agent  VARCHAR(512),
            CONSTRAINT songplays_pkey PRIMARY KEY (playid)
        ) DISTSTYLE KEY DISTKEY (start_time) SORTKEY (start_time);
    """

    user_table_create = """
        CREATE TABLE IF NOT EXISTS users (
            userid     INTEGER NOT NULL,
            first_name VARCHAR(256),
            last_name  VARCHAR(256),
            gender     CHAR(1),
            level      VARCHAR(256),
            CONSTRAINT users_pkey PRIMARY KEY (userid)
        ) SORTKEY (userid);
    """

    song_table_create = """
        CREATE TABLE IF NOT EXISTS songs (
            songid   VARCHAR(256) NOT NULL,
            title    VARCHAR(256),
            artistid VARCHAR(256),
            year     INTEGER,
            duration NUMERIC(10,5),
            CONSTRAINT songs_pkey PRIMARY KEY (songid)
        ) SORTKEY (songid);
    """

    artist_table_create = """
        CREATE TABLE IF NOT EXISTS artists (
            artistid  VARCHAR(256) NOT NULL,
            name      VARCHAR(256),
            location  VARCHAR(512),
            latitude  NUMERIC(9,6),
            longitude NUMERIC(9,6),
            CONSTRAINT artists_pkey PRIMARY KEY (artistid)
        ) SORTKEY (artistid);
    """

    time_table_create = """
        CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP NOT NULL,
            hour       INTEGER,
            day        INTEGER,
            week       INTEGER,
            month      INTEGER,
            year       INTEGER,
            weekday    INTEGER,
            CONSTRAINT time_pkey PRIMARY KEY (start_time)
        ) DISTSTYLE KEY DISTKEY (start_time) SORTKEY (start_time);
    """

    # ──────────────────────────────────────────────
    # INSERT statements (fact + dimension transforms)
    # ──────────────────────────────────────────────

    songplay_table_insert = """
        SELECT
            md5(events.sessionid || events.start_time) AS playid,
            events.start_time,
            events.userid,
            events.level,
            songs.song_id   AS songid,
            songs.artist_id AS artistid,
            events.sessionid,
            events.location,
            events.useragent AS user_agent
        FROM (
            SELECT
                TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                *
            FROM staging_events
            WHERE page = 'NextSong'
        ) events
        LEFT JOIN staging_songs songs
            ON events.song   = songs.title
           AND events.artist = songs.artist_name
           AND events.length = songs.duration
    """

    user_table_insert = """
        SELECT DISTINCT
            userid,
            firstName  AS first_name,
            lastName   AS last_name,
            gender,
            level
        FROM staging_events
        WHERE page = 'NextSong'
          AND userid IS NOT NULL
    """

    song_table_insert = """
        SELECT DISTINCT
            song_id   AS songid,
            title,
            artist_id AS artistid,
            year,
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
    """

    artist_table_insert = """
        SELECT DISTINCT
            artist_id        AS artistid,
            artist_name      AS name,
            artist_location  AS location,
            artist_latitude  AS latitude,
            artist_longitude AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
    """

    time_table_insert = """
        SELECT
            start_time,
            EXTRACT(hour    FROM start_time) AS hour,
            EXTRACT(day     FROM start_time) AS day,
            EXTRACT(week    FROM start_time) AS week,
            EXTRACT(month   FROM start_time) AS month,
            EXTRACT(year    FROM start_time) AS year,
            EXTRACT(dow     FROM start_time) AS weekday
        FROM songplays
    """
