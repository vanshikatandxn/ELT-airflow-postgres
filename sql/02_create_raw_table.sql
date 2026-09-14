CREATE TABLE IF NOT EXISTS raw.weather_raw (
    id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    latitude NUMERIC NOT NULL,
    longitude NUMERIC NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    temperature_c NUMERIC,
    windspeed_kmh NUMERIC,
    weathercode INTEGER,
    ingested_at TIMESTAMP NOT NULL DEFAULT now(),
    dag_run_date DATE NOT NULL
);
