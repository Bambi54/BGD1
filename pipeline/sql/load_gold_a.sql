-- =============================================================
--  GOLD LAYER A — zone_demand_trend
--  Aggregation: hourly trip volume and fare metrics per
--  pickup zone per day.
--  Use case: time-series demand forecasting by zone,
--            surge zone heatmaps.
-- =============================================================
 
CREATE TABLE IF NOT EXISTS gold.zone_demand_trend (
    pickup_zone_id  INT,
    trip_date       DATE,
    hour_of_day     INT,
    trip_count      BIGINT,
    avg_fare        NUMERIC(10,2),
    avg_distance    NUMERIC(10,2),
    avg_tip_pct     NUMERIC(10,4),
    PRIMARY KEY (pickup_zone_id, trip_date, hour_of_day)
);

-- =====================================================
-- STAGING AGGREGATION (fresh compute each run)
-- =====================================================

WITH agg AS (
    SELECT
        pickup_zone_id,
        DATE(pickup_at) AS trip_date,
        EXTRACT(HOUR FROM pickup_at)::INT AS hour_of_day,
        COUNT(*) AS trip_count,
        ROUND(AVG(fare_amount)::NUMERIC, 2) AS avg_fare,
        ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance,
        ROUND(
            AVG(
                CASE WHEN fare_amount > 0
                THEN tip_amount / fare_amount
                ELSE NULL END
            )::NUMERIC, 4
        ) AS avg_tip_pct
    FROM silver.clean_trips
    WHERE cab_type IN ('yellow', 'green')
      AND pickup_zone_id IS NOT NULL
    GROUP BY pickup_zone_id, DATE(pickup_at), EXTRACT(HOUR FROM pickup_at)
)

-- =====================================================
-- UPSERT (MERGE)
-- =====================================================

MERGE INTO gold.zone_demand_trend t
USING agg s
ON  t.pickup_zone_id = s.pickup_zone_id
AND t.trip_date      = s.trip_date
AND t.hour_of_day    = s.hour_of_day

WHEN MATCHED THEN
    UPDATE SET
        trip_count   = s.trip_count,
        avg_fare     = s.avg_fare,
        avg_distance = s.avg_distance,
        avg_tip_pct  = s.avg_tip_pct

WHEN NOT MATCHED THEN
    INSERT (
        pickup_zone_id,
        trip_date,
        hour_of_day,
        trip_count,
        avg_fare,
        avg_distance,
        avg_tip_pct
    )
    VALUES (
        s.pickup_zone_id,
        s.trip_date,
        s.hour_of_day,
        s.trip_count,
        s.avg_fare,
        s.avg_distance,
        s.avg_tip_pct
    );