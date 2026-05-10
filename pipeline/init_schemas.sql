-- init_schemas.sql
-- Runs automatically on first PostgreSQL container start.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE bronze.raw_trips (
    -- metadata
    cab_type                  VARCHAR(20),  -- yellow, green, fhv
    file_year                 INTEGER,
    file_month                INTEGER,

    -- common taxi trip columns (yellow + green)
    vendor_id                 VARCHAR(20),
    pickup_datetime           TEXT,
    dropoff_datetime          TEXT,
    passenger_count           VARCHAR(20),
    trip_distance             VARCHAR(20),

    -- location columns
    pulocation_id             VARCHAR(20),
    dolocation_id             VARCHAR(20),

    -- rate/payment info
    ratecode_id               VARCHAR(20),
    store_and_fwd_flag        VARCHAR(20),
    payment_type              VARCHAR(20),

    -- fare breakdown
    fare_amount               VARCHAR(20),
    extra                     VARCHAR(20),
    mta_tax                   VARCHAR(20),
    improvement_surcharge     VARCHAR(20),
    tip_amount                VARCHAR(20),
    tolls_amount              VARCHAR(20),
    total_amount              VARCHAR(20),
    congestion_surcharge      VARCHAR(20),
    airport_fee               VARCHAR(20),
    cbd_congestion_fee        VARCHAR(20),

    -- green taxi specific
    trip_type                 VARCHAR(20),
    ehail_fee                 VARCHAR(20),

    -- FHV specific
    dispatching_base_num      VARCHAR(20),
    affiliated_base_number    VARCHAR(20),
    sr_flag                   VARCHAR(20),

    -- optional ingestion metadata
    created_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);