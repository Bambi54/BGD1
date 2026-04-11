DROP TABLE IF EXISTS silver.clean_trips CASCADE;
 
CREATE TABLE silver.clean_trips AS
WITH yellow AS (
    SELECT
        'yellow'                                        AS cab_type,
        tpep_pickup_datetime::TIMESTAMP                 AS pickup_at,
        tpep_dropoff_datetime::TIMESTAMP                AS dropoff_at,
        CAST(
            NULLIF(PULocationID, '') AS DOUBLE PRECISION
        )::INT                                          AS pickup_zone_id,
        CAST(
            NULLIF(DOLocationID, '') AS DOUBLE PRECISION
        )::INT                                          AS dropoff_zone_id,
        NULLIF(trip_distance, '')::REAL                 AS trip_distance,
        NULLIF(fare_amount, '')::REAL                   AS fare_amount,
        NULLIF(tip_amount, '')::REAL                    AS tip_amount,
        NULLIF(total_amount, '')::REAL                  AS total_amount,
        CAST(
            NULLIF(passenger_count, '') AS DOUBLE PRECISION
        )::INT                                          AS passenger_count
    FROM bronze.raw_yellow_trips
    WHERE tpep_pickup_datetime  ~ '^\d{4}-\d{2}-\d{2}'
      AND tpep_dropoff_datetime ~ '^\d{4}-\d{2}-\d{2}'
      AND NULLIF(fare_amount,    '')::REAL > 0
      AND NULLIF(trip_distance,  '')::REAL > 0
      AND NULLIF(total_amount,   '')::REAL > 0
),
green AS (
    SELECT
        'green'                                         AS cab_type,
        lpep_pickup_datetime::TIMESTAMP                 AS pickup_at,
        lpep_dropoff_datetime::TIMESTAMP                AS dropoff_at,
        CAST(
            NULLIF(PULocationID, '') AS DOUBLE PRECISION
        )::INT                                          AS pickup_zone_id,
        CAST(
            NULLIF(DOLocationID, '') AS DOUBLE PRECISION
        )::INT                                          AS dropoff_zone_id,
        NULLIF(trip_distance, '')::REAL                 AS trip_distance,
        NULLIF(fare_amount,   '')::REAL                 AS fare_amount,
        NULLIF(tip_amount,    '')::REAL                 AS tip_amount,
        NULLIF(total_amount,  '')::REAL                 AS total_amount,
        CAST(
            NULLIF(passenger_count, '') AS DOUBLE PRECISION
        )::INT                                          AS passenger_count
    FROM bronze.raw_green_trips
    WHERE lpep_pickup_datetime  ~ '^\d{4}-\d{2}-\d{2}'
      AND lpep_dropoff_datetime ~ '^\d{4}-\d{2}-\d{2}'
      AND NULLIF(fare_amount,    '')::REAL > 0
      AND NULLIF(trip_distance,  '')::REAL > 0
      AND NULLIF(total_amount,   '')::REAL > 0
),
fhv AS (
    SELECT
        'fhv'                                           AS cab_type,
        pickup_datetime::TIMESTAMP                      AS pickup_at,
        dropOff_datetime::TIMESTAMP                     AS dropoff_at,
        CAST(
            NULLIF(PULocationID, '') AS DOUBLE PRECISION
        )::INT                                          AS pickup_zone_id,
        CAST(
            NULLIF(DOLocationID, '') AS DOUBLE PRECISION
        )::INT                                          AS dropoff_zone_id,
        NULL::REAL                                      AS trip_distance,
        NULL::REAL                                      AS fare_amount,
        NULL::REAL                                      AS tip_amount,
        NULL::REAL                                      AS total_amount,
        NULL::INT                                       AS passenger_count
    FROM bronze.raw_fhv_trips
    WHERE pickup_datetime  ~ '^\d{4}-\d{2}-\d{2}'
      AND dropOff_datetime ~ '^\d{4}-\d{2}-\d{2}'
),
all_trips AS (
    SELECT * FROM yellow
    UNION ALL
    SELECT * FROM green
    UNION ALL
    SELECT * FROM fhv
)
SELECT DISTINCT ON (cab_type, pickup_at, dropoff_at, pickup_zone_id)
    ROW_NUMBER() OVER ()                                            AS trip_id,
    cab_type,
    pickup_at,
    dropoff_at,
    pickup_zone_id,
    dropoff_zone_id,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    passenger_count,
    ROUND(
        EXTRACT(EPOCH FROM (dropoff_at - pickup_at)) / 60.0
    )::INT                                                          AS trip_duration_min,
    NOW()                                                           AS loaded_at
FROM all_trips
WHERE pickup_at  >= '2019-01-01'
  AND pickup_at  <  '2025-01-01'
  AND dropoff_at >  pickup_at
  AND EXTRACT(EPOCH FROM (dropoff_at - pickup_at)) / 60.0
      BETWEEN 1 AND 300
  AND pickup_zone_id  BETWEEN 1 AND 263
  AND dropoff_zone_id BETWEEN 1 AND 263
ORDER BY cab_type, pickup_at, dropoff_at, pickup_zone_id;
 
CREATE INDEX idx_silver_pickup_at   ON silver.clean_trips (pickup_at);
CREATE INDEX idx_silver_pu_zone     ON silver.clean_trips (pickup_zone_id);
CREATE INDEX idx_silver_cab_type    ON silver.clean_trips (cab_type);