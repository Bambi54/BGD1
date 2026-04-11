-- =============================================================
--  GOLD LAYER A — zone_demand_trend
--  Aggregation: hourly trip volume and fare metrics per
--  pickup zone per day.
--  Use case: time-series demand forecasting by zone,
--            surge zone heatmaps.
-- =============================================================
 
DROP TABLE IF EXISTS gold.zone_demand_trend CASCADE;
 
CREATE TABLE gold.zone_demand_trend AS
SELECT
    pickup_zone_id,
    DATE(pickup_at)                                     AS trip_date,
    EXTRACT(HOUR FROM pickup_at)::INT                   AS hour_of_day,
    COUNT(*)                                            AS trip_count,
    ROUND(AVG(fare_amount)::NUMERIC,   2)               AS avg_fare,
    ROUND(AVG(trip_distance)::NUMERIC, 2)               AS avg_distance,
    ROUND(
        AVG(
            CASE
                WHEN fare_amount > 0 THEN tip_amount / fare_amount
                ELSE NULL
            END
        )::NUMERIC, 4
    )                                                   AS avg_tip_pct
FROM silver.clean_trips
WHERE cab_type IN ('yellow', 'green')
  AND pickup_zone_id IS NOT NULL
GROUP BY pickup_zone_id, DATE(pickup_at), EXTRACT(HOUR FROM pickup_at)
ORDER BY pickup_zone_id, trip_date, hour_of_day;
 
 
-- =============================================================
--  GOLD LAYER B — hourly_fare_profile
--  JOIN + aggregation: average fare, tip, and duration by
--  cab type × day-of-week × hour. Derives a surge_index
--  comparing each slot to that cab type's overall average.
--  Use case: surge pricing window forecasting.
-- =============================================================
 
DROP TABLE IF EXISTS gold.hourly_fare_profile CASCADE;
 
CREATE TABLE gold.hourly_fare_profile AS
WITH slot_stats AS (
    SELECT
        cab_type,
        EXTRACT(DOW  FROM pickup_at)::INT               AS day_of_week,
        EXTRACT(HOUR FROM pickup_at)::INT               AS hour_of_day,
        COUNT(*)                                        AS trip_count,
        AVG(fare_amount)                                AS avg_fare,
        AVG(tip_amount)                                 AS avg_tip_amount,
        AVG(trip_duration_min)                          AS avg_duration_min
    FROM silver.clean_trips
    WHERE cab_type IN ('yellow', 'green')
      AND fare_amount IS NOT NULL
    GROUP BY cab_type,
             EXTRACT(DOW  FROM pickup_at),
             EXTRACT(HOUR FROM pickup_at)
),
type_baseline AS (
    SELECT
        cab_type,
        AVG(fare_amount)  AS baseline_fare
    FROM silver.clean_trips
    WHERE cab_type IN ('yellow', 'green')
      AND fare_amount IS NOT NULL
    GROUP BY cab_type
)
SELECT
    s.cab_type,
    s.day_of_week,
    s.hour_of_day,
    s.trip_count,
    ROUND(s.avg_fare::NUMERIC,         2)               AS avg_fare,
    ROUND(s.avg_tip_amount::NUMERIC,   2)               AS avg_tip_amount,
    ROUND(s.avg_duration_min::NUMERIC, 1)               AS avg_duration_min,
    ROUND(
        (s.avg_fare / NULLIF(b.baseline_fare, 0))::NUMERIC, 3
    )                                                   AS surge_index
FROM slot_stats s
JOIN type_baseline b ON s.cab_type = b.cab_type
ORDER BY s.cab_type, s.day_of_week, s.hour_of_day;
 
 
-- =============================================================
--  GOLD LAYER C — driver_revenue_rank
--  Window function: rank FHV dispatching bases by daily
--  revenue (total_amount) within each calendar date.
--  Use case: identify top-performing bases, flag underperformers.
--  Note: FHV fare data is sparse, ranks are by trip volume
--        where fare is unavailable.
-- =============================================================
 
DROP TABLE IF EXISTS gold.driver_revenue_rank CASCADE;
 
CREATE TABLE gold.driver_revenue_rank AS
WITH base_daily AS (
    SELECT
        f.dispatching_base_num,
        DATE(t.pickup_at)                               AS trip_date,
        COUNT(*)                                        AS trip_count,
        ROUND(COALESCE(SUM(t.total_amount), 0)::NUMERIC, 2)
                                                        AS total_revenue,
        ROUND(COALESCE(AVG(t.total_amount), 0)::NUMERIC, 2)
                                                        AS avg_fare
    FROM silver.clean_trips t
    JOIN bronze.raw_fhv_trips f
      ON t.pickup_at::DATE  = f.pickup_datetime::DATE
     AND t.pickup_zone_id   = CAST(NULLIF(f.PULocationID,'') AS DOUBLE PRECISION)::INT
     AND t.cab_type         = 'fhv'
    WHERE f.dispatching_base_num IS NOT NULL
    GROUP BY f.dispatching_base_num, DATE(t.pickup_at)
)
SELECT
    dispatching_base_num,
    trip_date,
    trip_count,
    total_revenue,
    avg_fare,
    RANK() OVER (
        PARTITION BY trip_date
        ORDER BY trip_count DESC
    )                                                   AS revenue_rank
FROM base_daily
ORDER BY trip_date, revenue_rank;