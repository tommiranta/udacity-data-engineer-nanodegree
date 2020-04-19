class SqlQueries:
    """
    The SqlQueries class contain SQL query templates that are
    dynamically formatted during DAG task definition.
    """
    staging_trips_create = ("""
        CREATE TABLE IF NOT EXISTS public.{table} (
            taxi_id INTEGER,
            trip_start_timestamp TIMESTAMP,
            trip_end_timestamp TIMESTAMP,
            trip_seconds INTEGER,
            trip_miles NUMERIC(8,2),
            pickup_census_tract INTEGER,
            dropoff_census_tract INTEGER,
            pickup_community_area INTEGER,
            dropoff_community_area INTEGER,
            fare NUMERIC(8,2),
            tips NUMERIC(8,2),
            tolls NUMERIC(8,2),
            extras NUMERIC(8,2),
            trip_total NUMERIC(8,2),
            payment_type VARCHAR(256),
            company INTEGER,
            pickup_latitude INTEGER,
            pickup_longitude INTEGER,
            dropoff_latitude INTEGER,
            dropoff_longitude INTEGER
        );
    """)

    staging_precipitation_create = ("""
        CREATE TABLE IF NOT EXISTS public.{table} (
            station_id VARCHAR(32) NOT NULL,
            "datetime" VARCHAR(8) NOT NULL,
            element VARCHAR(4),
            "value" NUMERIC(8,2),
            measurement VARCHAR(2),
            quality VARCHAR(2),
            source VARCHAR(2),
            time VARCHAR(4)
        );
    """)

    staging_holidays_create = ("""
        CREATE TABLE IF NOT EXISTS public.{table} (
            holiday_id INTEGER NOT NULL,
            "datetime" TIMESTAMP NOT NULL,
            holiday VARCHAR(128)
        );
    """)

    table_drop = ("""
        DROP TABLE public.{table}
    """)

    staging_copy = ("""
        TRUNCATE TABLE {table};
        COPY {table}
        FROM '{s3path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        FORMAT AS CSV
        DELIMITER AS '{delimiter}'
        IGNOREHEADER {ignoreheader}
    """)

    dim_time_insert = ("""
        INSERT INTO {dest_table} (
            "datetime",
            "year",
            "month",
            "day",
            "hour",
            "quarter",
            week,
            weekday,
            holiday
        )
        SELECT DISTINCT
            h.datetime,
            EXTRACT(YEAR FROM h.datetime) as year,
            EXTRACT(MONTH FROM h.datetime) as month,
            EXTRACT(DAY FROM h.datetime) as day,
            EXTRACT(HOUR FROM h.datetime) as hour,
            EXTRACT(WEEKDAY FROM h.datetime) as "weekday",
            EXTRACT(QUARTER FROM h.datetime) as quarter,
            EXTRACT(WEEK FROM h.datetime) as week,
            holiday
        FROM staging_weather_humidity h
        LEFT JOIN staging_holidays hd
            ON (EXTRACT(YEAR FROM h.datetime) = EXTRACT(YEAR FROM hd.datetime)
            AND EXTRACT(MONTH FROM h.datetime) = EXTRACT(MONTH FROM hd.datetime)
            AND EXTRACT(DAY FROM h.datetime) = EXTRACT(DAY FROM hd.datetime))
        WHERE
            h.datetime BETWEEN '2016-01-01 00:00' AND '2016-12-31 23:00'
        ORDER BY h.datetime ASC
    """)

    dim_payment_type_insert = ("""
        INSERT INTO {dest_table} (
            payment_type
        )
        SELECT DISTINCT
            payment_type
        FROM {source_table}
        WHERE payment_type NOT IN (SELECT DISTINCT payment_type FROM {dest_table})
    """)

    fact_weather_insert = ("""
        INSERT INTO {dest_table} (
            time_id,
            humidity,
            pressure,
            temperature,
            weather_description,
            wind_direction,
            wind_speed,
            precipitation
        )
        SELECT
            t.time_id,
            hum.chicago as humidity,
            pres.chicago as pressure,
            temp.chicago * 1.8 - 459.67 as temperature,
            wdesc.chicago as weather_description,
            wind.chicago as wind_direction,
            wins.chicago as wind_speed,
            precip.value as precipitation
        FROM
            staging_weather_humidity hum
        INNER JOIN staging_weather_pressure pres
            ON (hum.datetime = pres.datetime)
        INNER JOIN staging_weather_temperature temp
            ON (hum.datetime = temp.datetime)
        INNER JOIN staging_weather_weather_description wdesc
            ON (hum.datetime = wdesc.datetime)
        INNER JOIN staging_weather_wind_direction wind
            ON (hum.datetime = wind.datetime)
        INNER JOIN staging_weather_wind_speed wins
            ON (hum.datetime = wins.datetime)
        INNER JOIN dim_time t
            ON (hum.datetime = t.datetime)
        LEFT JOIN (SELECT
                        *,
                        TO_TIMESTAMP("datetime", 'YYYYMMDD') as date_time,
                        EXTRACT(HOUR FROM date_time)
                    FROM
                        staging_weather_precipitation
                    WHERE
                        station_id = 'USC00111577'
                        AND element = 'PRCP'
                    ORDER BY
                        date_time) precip
            ON (EXTRACT(YEAR FROM hum.datetime) = EXTRACT(YEAR FROM precip.date_time)
                AND EXTRACT(MONTH FROM hum.datetime) = EXTRACT(MONTH FROM precip.date_time)
                AND EXTRACT(DAY FROM hum.datetime) = EXTRACT(DAY FROM precip.date_time)
                AND EXTRACT(HOUR FROM hum.datetime) = EXTRACT(HOUR FROM precip.date_time))
        WHERE hum.datetime >= '2016-01-01'
    """)

    fact_trips_insert = ("""
        INSERT INTO {dest_table} (
            time_id,
            payment_type_id,
            taxi_id,
            trip_start_timestamp,
            trip_end_timestamp,
            trip_seconds,
            trip_miles,
            pickup_census_tract,
            dropoff_census_tract,
            pickup_community_area,
            dropoff_community_area,
            fare,
            tips,
            tolls,
            extras,
            trip_total,
            company,
            pickup_latitude,
            pickup_longitude,
            dropoff_latitude,
            dropoff_longitude
        )
        SELECT
            time_id,
            payment_type_id,
            taxi_id,
            trip_start_timestamp,
            trip_end_timestamp,
            trip_seconds,
            trip_miles,
            pickup_census_tract,
            dropoff_census_tract,
            pickup_community_area,
            dropoff_community_area,
            fare,
            tips,
            tolls,
            extras,
            trip_total,
            company,
            pickup_latitude,
            pickup_longitude,
            dropoff_latitude,
            dropoff_longitude
        FROM
            {source_table} tr
        INNER JOIN dim_time ti
            ON (EXTRACT(YEAR FROM tr.trip_start_timestamp) = ti.year
                AND EXTRACT(MONTH FROM tr.trip_start_timestamp) = ti.month
                AND EXTRACT(DAY FROM tr.trip_start_timestamp) = ti.day
                AND EXTRACT(HOUR FROM tr.trip_start_timestamp) = ti.hour)
        INNER JOIN dim_payment_types p
            ON (tr.payment_type = p.payment_type)
    """)

    fact_daily_insert = ("""
        INSERT INTO public.{dest_table} (
            "datetime",
            "year",
            "month",
            "day",
            trip_amount,
            trip_duration_min,
            trip_duration_avg,
            trip_duration_max,
            trip_duration_sum,
            trip_miles_min,
            trip_miles_avg,
            trip_miles_max,
            trip_miles_sum,
            trip_cost_min,
            trip_cost_avg,
            trip_cost_max,
            trip_cost_sum
        )
        SELECT
            CAST(MIN(trip_start_timestamp) AS DATE) AS "datetime",
            EXTRACT(YEAR FROM trip_start_timestamp) AS "year",
            EXTRACT(MONTH FROM trip_start_timestamp) AS "month",
            EXTRACT(DAY FROM trip_start_timestamp) AS "day",
            COUNT(*) AS trip_amount,
            MIN(trip_seconds) AS trip_duration_min,
            AVG(trip_seconds) AS trip_duration_avg,
            MAX(trip_seconds) AS trip_duration_max,
            SUM(trip_seconds) AS trip_duration_sum,
            MIN(trip_miles) AS trip_miles_min,
            AVG(trip_miles) AS trip_miles_avg,
            MAX(trip_miles) AS trip_miles_max,
            SUM(trip_miles) AS trip_duration_sum,
            MIN(trip_total) AS trip_cost_min,
            AVG(trip_total) AS trip_cost_avg,
            MAX(trip_total) AS trip_cost_max,
            SUM(trip_total) AS trip_cost_sum
        FROM {source_table}
        GROUP BY
            "year", "month", "day"
        ORDER BY
            "year", "month", "day"
    """)

    row_count = ("""
        SELECT COUNT(*)
        FROM {table}
    """)
