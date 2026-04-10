CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS bronze.raw_yellow_trips CASCADE;
CREATE TABLE bronze.raw_yellow_trips (
    row_id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    VendorID                TEXT,
    tpep_pickup_datetime    TEXT,
    tpep_dropoff_datetime   TEXT,
    passenger_count         TEXT,
    trip_distance           TEXT,
    RatecodeID              TEXT,
    store_and_fwd_flag      TEXT,
    PULocationID            TEXT,
    DOLocationID            TEXT,
    payment_type            TEXT,
    fare_amount             TEXT,
    extra                   TEXT,
    mta_tax                 TEXT,
    tip_amount              TEXT,
    tolls_amount            TEXT,
    improvement_surcharge   TEXT,
    total_amount            TEXT,
    congestion_surcharge    TEXT,
    airport_fee             TEXT
);
 
DROP TABLE IF EXISTS bronze.raw_green_trips CASCADE;
CREATE TABLE bronze.raw_green_trips (
    row_id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    VendorID                TEXT,
    lpep_pickup_datetime    TEXT,
    lpep_dropoff_datetime   TEXT,
    store_and_fwd_flag      TEXT,
    RatecodeID              TEXT,
    PULocationID            TEXT,
    DOLocationID            TEXT,
    passenger_count         TEXT,
    trip_distance           TEXT,
    fare_amount             TEXT,
    extra                   TEXT,
    mta_tax                 TEXT,
    tip_amount              TEXT,
    tolls_amount            TEXT,
    ehail_fee               TEXT,
    improvement_surcharge   TEXT,
    total_amount            TEXT,
    payment_type            TEXT,
    trip_type               TEXT,
    congestion_surcharge    TEXT
);
 
DROP TABLE IF EXISTS bronze.raw_fhv_trips CASCADE;
CREATE TABLE bronze.raw_fhv_trips (
    row_id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dispatching_base_num    TEXT,
    pickup_datetime         TEXT,
    dropOff_datetime        TEXT,
    PULocationID            TEXT,
    DOLocationID            TEXT,
    SR_Flag                 TEXT,
    Affiliated_base_number  TEXT
);