CREATE TABLE IF NOT EXISTS public.dim_time (
	time_id INTEGER IDENTITY(0,1),
	"datetime" TIMESTAMP NOT NULL,
	"year" INTEGER NOT NULL,
	"month" INTEGER NOT NULL,
	"day" INTEGER NOT NULL,
	"hour" INTEGER NOT NULL,
	quarter INTEGER NOT NULL,
	week INTEGER NOT NULL,
	"weekday" INTEGER NOT NULL,
	"holiday" VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS public.dim_payment_types (
	payment_type_id INTEGER IDENTITY(0,1),
	payment_type VARCHAR(128)
);

INSERT INTO public.dim_payment_types (payment_type)
VALUES ('Dummy');

CREATE TABLE IF NOT EXISTS public.fact_weather (
	weather_id INTEGER IDENTITY(0,1),
	time_id INTEGER NOT NULL,
	humidity NUMERIC(8,2),
	pressure NUMERIC(8,2),
	temperature NUMERIC(8,2),
	weather_description VARCHAR(256),
	wind_direction NUMERIC(8,2),
	wind_speed NUMERIC(8,2),
	precipitation NUMERIC(8,2)
);

CREATE TABLE IF NOT EXISTS public.fact_trips (
	trip_id INTEGER IDENTITY(0,1),
	time_id INTEGER NOT NULL,
	payment_type_id INTEGER NOT NULL,
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
	company INTEGER,
	pickup_latitude INTEGER,
	pickup_longitude INTEGER,
	dropoff_latitude INTEGER,
	dropoff_longitude INTEGER
);

CREATE TABLE IF NOT EXISTS public.fact_trips_daily (
	"datetime" DATE NOT NULL,
	"year" INTEGER NOT NULL,
	"month" INTEGER NOT NULL,
	"day" INTEGER NOT NULL,
	trip_amount INTEGER,
	trip_duration_min NUMERIC(8,2),
	trip_duration_avg NUMERIC(8,2),
	trip_duration_max NUMERIC(8,2),
	trip_duration_sum NUMERIC(12,2), 
	trip_miles_min NUMERIC(8,2),
	trip_miles_avg NUMERIC(8,2),
	trip_miles_max NUMERIC(8,2),
	trip_miles_sum NUMERIC(12,2),
	trip_cost_min NUMERIC(8,2),
	trip_cost_avg NUMERIC(8,2),
	trip_cost_max NUMERIC(8,2),
	trip_cost_sum NUMERIC(12,2)
);