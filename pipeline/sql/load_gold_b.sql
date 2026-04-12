-- =============================================================
--  GOLD LAYER B — hourly_fare_profile
--  JOIN + aggregation: average fare, tip, and duration by
--  cab type × day-of-week × hour. Derives a surge_index
--  comparing each slot to that cab type's overall average.
--  Use case: surge pricing window forecasting.
-- =============================================================
 
CREATE TABLE IF NOT EXISTS gold.hourly_fare_profile (
    cab_type TEXT,
    day_of_week INT,
    hour_of_day INT,
    trip_count BIGINT,
    avg_fare NUMERIC,
    avg_tip_amount NUMERIC,
    avg_duration_min NUMERIC,
    surge_index NUMERIC,
    PRIMARY KEY (cab_type, day_of_week, hour_of_day)
);

WITH slot_stats AS (
    SELECT
        cab_type,
        EXTRACT(DOW FROM pickup_at)::INT AS day_of_week,
        EXTRACT(HOUR FROM pickup_at)::INT AS hour_of_day,
        COUNT(*) AS trip_count,
        AVG(fare_amount) AS avg_fare,
        AVG(tip_amount) AS avg_tip_amount,
        AVG(trip_duration_min) AS avg_duration_min
    FROM silver.clean_trips
    WHERE cab_type IN ('yellow', 'green')
      AND fare_amount IS NOT NULL
    GROUP BY 1,2,3
),
type_baseline AS (
    SELECT
        cab_type,
        AVG(fare_amount) AS baseline_fare
    FROM silver.clean_trips
    WHERE cab_type IN ('yellow', 'green')
      AND fare_amount IS NOT NULL
    GROUP BY 1
),
final AS (
    SELECT
        s.cab_type,
        s.day_of_week,
        s.hour_of_day,
        s.trip_count,
        ROUND(s.avg_fare::NUMERIC, 2) AS avg_fare,
        ROUND(s.avg_tip_amount::NUMERIC, 2) AS avg_tip_amount,
        ROUND(s.avg_duration_min::NUMERIC, 1) AS avg_duration_min,
        ROUND((s.avg_fare / NULLIF(b.baseline_fare,0))::NUMERIC, 3) AS surge_index
    FROM slot_stats s
    JOIN type_baseline b USING (cab_type)
)

INSERT INTO gold.hourly_fare_profile
SELECT * FROM final
ON CONFLICT (cab_type, day_of_week, hour_of_day)
DO UPDATE SET
    trip_count = EXCLUDED.trip_count,
    avg_fare = EXCLUDED.avg_fare,
    avg_tip_amount = EXCLUDED.avg_tip_amount,
    avg_duration_min = EXCLUDED.avg_duration_min,
    surge_index = EXCLUDED.surge_index;