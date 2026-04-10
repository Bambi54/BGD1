# NYC Taxi & Rideshare Demand — ELT Medallion Pipeline
 
## Problem Statement
 
A city mobility analyst wants to understand how taxi and rideshare demand, fares, and surge patterns vary by pickup zone and time of day across NYC's five boroughs. The goal is to identify which zones and hour-of-day windows drive peak revenue and where surge pricing is most predictable — inputs for a demand forecasting model.
 
Raw TLC data arrives as per-month Parquet files split across three vehicle classes (yellow cab, green cab, FHV/rideshare), totalling 50 GB+ for 2019–2024. The medallion pipeline unifies, cleans, and aggregates these into Gold tables ready for BI dashboards and time-series models.
 
**Analytical questions the Gold layer answers:**
- Which pickup zones have the highest and most consistent trip demand by hour?
- At what times of day and days of week does the surge index peak for yellow vs green cabs?
- Which FHV dispatching bases generate the most trip volume on a given day?
 
---
 
## Dataset
 
**NYC TLC Trip Record Data** (public domain, no account required)
- Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Format: Parquet files, one per month per vehicle class
- Size: ~50 GB+ for 2019–2024 across all three types
- Vehicle classes: Yellow Taxi, Green Taxi, For-Hire Vehicle (FHV/rideshare)
- Zone lookup: 263 taxi zones mapped to NYC boroughs and neighbourhoods
 
---
 
### Table catalogue
 
| Layer  | Table                 | Est. rows (2023) | Key transform                         |
|--------|-----------------------|------------------|---------------------------------------|
| Bronze | raw_yellow_trips      | ~38M             | Raw TEXT ingest via Parquet           |
| Bronze | raw_green_trips       | ~1.3M            | Raw TEXT ingest via Parquet           |
| Bronze | raw_fhv_trips         | ~76M             | Raw TEXT ingest via Parquet           |
| Silver | clean_trips           | ~100M            | UNION + CAST + FILTER + DEDUP        |
| Gold A | zone_demand_trend     | ~2.3M            | GROUP BY zone × date × hour           |
| Gold B | hourly_fare_profile   | ~336 rows        | JOIN + GROUP BY cab × dow × hour      |
| Gold C | driver_revenue_rank   | ~50K             | RANK() OVER (PARTITION BY trip_date)  |
 
---
 
## Quickstart
 
```bash
# 1. Start PostgreSQL
docker run --name taxipg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=taxidb -p 5432:5432 postgres:15
 
# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the pipeline (defaults to 2023 only)
python bronze/load_bronze.py
python silver/load_silver.py
python gold/load_gold.py

# Load a wider range:
START_YEAR=2021 END_YEAR=2023 python bronze/load_bronze.py

# To connect to the db manually:
docker exec -it taxipg psql -U postgres taxidb
```

Expected runtime: ~30 min per year of data
 
---
 
## Key columns & derivations
 
| Column              | Layer  | Derivation                                            |
|---------------------|--------|-------------------------------------------------------|
| `trip_duration_min` | Silver | `(dropoff_at - pickup_at)` in minutes                 |
| `avg_tip_pct`       | Gold A | `tip_amount / fare_amount` averaged per zone-hour     |
| `surge_index`       | Gold B | `slot_avg_fare / cab_type_baseline_fare`               |
| `revenue_rank`      | Gold C | `RANK() OVER (PARTITION BY trip_date ORDER BY trip_count DESC)` |
 
---
 
## Silver filter criteria
 
Rows excluded from `clean_trips` if any apply:
- `fare_amount`, `trip_distance`, or `total_amount` ≤ 0
- `dropoff_at` ≤ `pickup_at`
- Trip duration outside 1–300 minutes
- Pickup or dropoff zone ID outside 1–263
- Pickup year outside 2019–2024