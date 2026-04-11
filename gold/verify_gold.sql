-- Top 10 busiest pickup zones on a Monday morning rush
SELECT pickup_zone_id, ROUND(AVG(trip_count),2) AS avg_trips
FROM gold.zone_demand_trend
WHERE EXTRACT(DOW FROM trip_date) = 1
    AND hour_of_day BETWEEN 7 AND 9
GROUP BY pickup_zone_id
ORDER BY avg_trips DESC
LIMIT 10;
 

-- Surge index by hour for yellow cabs on Fridays
SELECT hour_of_day, avg_fare, surge_index
FROM gold.hourly_fare_profile
WHERE cab_type = 'yellow' AND day_of_week = 5
ORDER BY surge_index DESC;
 

-- Top 5 FHV bases by trip volume on a given date
SELECT dispatching_base_num, trip_count, revenue_rank
FROM gold.driver_revenue_rank
WHERE trip_date = '2023-07-04'
ORDER BY revenue_rank
LIMIT 5;
