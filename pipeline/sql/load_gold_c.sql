-- =============================================================
--  GOLD LAYER C — driver_revenue_rank
--  Window function: rank FHV dispatching bases by daily
--  revenue (total_amount) within each calendar date.
--  Use case: identify top-performing bases, flag underperformers.
--  Note: FHV fare data is sparse, ranks are by trip volume
--        where fare is unavailable.
-- =============================================================
 
CREATE TABLE IF NOT EXISTS gold.driver_revenue_rank (
    dispatching_base_num TEXT,
    trip_date DATE,
    trip_count BIGINT,
    total_revenue NUMERIC,
    avg_fare NUMERIC,
    revenue_rank INT,
    PRIMARY KEY (dispatching_base_num, trip_date)
);

WITH base_daily AS (
    SELECT
        dispatching_base_num,
        DATE(pickup_at) AS trip_date,
        COUNT(*) AS trip_count,
        COALESCE(SUM(total_amount), 0) AS total_revenue,
        COALESCE(AVG(total_amount), 0) AS avg_fare
    FROM silver.clean_trips
    WHERE cab_type = 'fhv'
      AND dispatching_base_num IS NOT NULL
    GROUP BY 1,2
),
ranked AS (
    SELECT *,
           RANK() OVER (
               PARTITION BY trip_date
               ORDER BY trip_count DESC
           ) AS revenue_rank
    FROM base_daily
)

INSERT INTO gold.driver_revenue_rank
SELECT * FROM ranked
ON CONFLICT (dispatching_base_num, trip_date)
DO UPDATE SET
    trip_count = EXCLUDED.trip_count,
    total_revenue = EXCLUDED.total_revenue,
    avg_fare = EXCLUDED.avg_fare,
    revenue_rank = EXCLUDED.revenue_rank;