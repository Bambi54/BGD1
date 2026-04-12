-- row count/distribution
SELECT
    cab_type,
    COUNT(*) AS total_rows,
    MIN(pickup_at) AS min_pickup,
    MAX(pickup_at) AS max_pickup
FROM silver.clean_trips
GROUP BY cab_type
ORDER BY total_rows DESC;

-- null/completeness check
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE pickup_at IS NULL)        AS null_pickup,
    COUNT(*) FILTER (WHERE dropoff_at IS NULL)       AS null_dropoff,
    COUNT(*) FILTER (WHERE pickup_zone_id IS NULL)   AS null_pu_zone,
    COUNT(*) FILTER (WHERE dropoff_zone_id IS NULL)  AS null_do_zone,
    COUNT(*) FILTER (WHERE trip_distance IS NULL)    AS null_distance,
    COUNT(*) FILTER (WHERE fare_amount IS NULL)      AS null_fare
FROM silver.clean_trips;

-- data sanity (should be 0)
SELECT *
FROM silver.clean_trips
WHERE
    trip_distance <= 0
    OR fare_amount <= 0
    OR total_amount <= 0
    OR trip_duration_min <= 0
    OR trip_duration_min > 300
LIMIT 50;

-- duration consistency
SELECT
    trip_id,
    pickup_at,
    dropoff_at,
    trip_duration_min,
    EXTRACT(EPOCH FROM (dropoff_at - pickup_at)) / 60 AS actual_duration
FROM silver.clean_trips
WHERE ABS(
    trip_duration_min - EXTRACT(EPOCH FROM (dropoff_at - pickup_at)) / 60
) > 1
LIMIT 50;

-- check duplicates
SELECT
    cab_type,
    pickup_at,
    dropoff_at,
    pickup_zone_id,
    COUNT(*) AS cnt
FROM silver.clean_trips
GROUP BY 1,2,3,4
HAVING COUNT(*) > 1
LIMIT 50;

-- distribution sanity
SELECT
    cab_type,
    ROUND(AVG(trip_distance)) AS avg_distance,
    ROUND(AVG(fare_amount))   AS avg_fare,
    ROUND(AVG(trip_duration_min)) AS avg_duration
FROM silver.clean_trips
GROUP BY cab_type;


-- check trips outside NYC
SELECT *
FROM silver.clean_trips
WHERE pickup_zone_id NOT BETWEEN 1 AND 263
   OR dropoff_zone_id NOT BETWEEN 1 AND 263
LIMIT 50;