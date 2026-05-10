# NYC Taxi & Rideshare Demand — ELT Medallion Pipeline
 
## Problem Statement
 
A city mobility analyst wants to understand how taxi and rideshare demand, fares, and surge patterns vary by pickup zone and time of day across NYC's five boroughs. The goal is to identify which zones and hour-of-day windows drive peak revenue and where surge pricing is most predictable — inputs for a demand forecasting model.
 
Raw TLC data arrives as per-month Parquet files split across three vehicle classes (yellow cab, green cab, FHV/rideshare), totalling 50 GB+ for 2019–2024. The medallion pipeline unifies, cleans, and aggregates these into Gold tables ready for BI dashboards and time-series models.
 
**Analytical questions the Gold layer answers:**
- Which pickup zones have the highest and most consistent trip demand by hour?
- At what times of day and days of week does the surge index peak for yellow vs green cabs?
- Which FHV dispatching bases generate the most trip volume on a given day?
---
 
## Stack
 
| Layer | Tool | Version |
|---|---|---|
| Orchestration | Dagster | 1.9.x |
| Processing | PySpark | 3.5.1 |
| Warehouse | PostgreSQL | 15 |
| Message queue | Apache Kafka | 7.6.x (Confluent) |
| Containerisation | Docker Compose | v2 |
| Source data | NYC TLC (public) | 2019–2024 |
 
---
 
## Architecture
 
The pipeline supports two ingestion modes that share the same Silver and Gold layers:
 
```
┌─────────────────────────────────────────────────────────┐
│                    NYC TLC (CloudFront)                  │
│          yellow_tripdata · green_tripdata · fhv          │
└───────────────────────────┬─────────────────────────────┘
                            │  HEAD poll every 6 h
                ┌───────────▼────────────┐
                │  tlc_new_file_sensor   │  Dagster sensor
                │  cursor-based dedup    │  (auto-trigger)
                └───────────┬────────────┘
                            │  RunRequest per new file
          ┌─────────────────┴──────────────────┐
          │  Kafka pipeline (streaming)         │  Batch pipeline
          │                                     │
          ▼                                     ▼
  ┌───────────────┐                   ┌──────────────────┐
  │ download_trips│                   │  raw_trips_batch │
  │  + Kafka      │                   │  direct ingest   │
  │  Producer     │                   │  (cron 03:00)    │
  └──────┬────────┘                   └────────┬─────────┘
         │ tlc_raw_files topic                 │
         │ key=cab · 3 partitions              │
  ┌──────▼────────┐                            │
  │   raw_trips   │                            |
  │ Kafka Consumer│    bronze.raw_trips        |
  │ manual commit │                            |
  └──────┬────────┘                            |
         │                                     |
  ┌──────▼────────┐    silver.clean_trips      |
  │  clean_trips  │◄───────────────────────────┘        
  └──────┬────────┘
         │
   ┌─────┼──────┐
   ▼     ▼      ▼
Gold A  Gold B  Gold C     PostgreSQL 15
```
 
### Kafka topic design
 
| Property | Value |
|---|---|
| Topic | `tlc_raw_files` |
| Partitions | 3 (one per cab type) |
| Message key | cab type (`yellow` / `green` / `fhv`) |
| Message value | JSON envelope `{cab, year, month, local_path}` |
| Offset commit | Manual — after successful Spark JDBC write only |
| Delivery guarantee | At-least-once (failed writes retry on next run) |
 
Keying by cab type guarantees all messages for the same vehicle class land on the same partition, preventing schema mismatches when messages from concurrent runs interleave.
 
---
 
## Dataset
 
**NYC TLC Trip Record Data** (public domain, no account required)
- Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Format: Parquet files, one per month per vehicle class
- Size: ~50 GB+ for 2019–2024 across all three types
- Vehicle classes: Yellow Taxi, Green Taxi, For-Hire Vehicle (FHV/rideshare)
- Zone lookup: 263 taxi zones mapped to NYC boroughs and neighbourhoods
---
 
## Table catalogue
 
| Layer  | Table                | Est. rows (2023) | Key transform                        |
|--------|----------------------|------------------|--------------------------------------|
| Bronze | raw_trips            | ~115M            | Raw TEXT ingest via Parquet (unified)|
| Silver | clean_trips          | ~100M            | UNION + CAST + FILTER + DEDUP        |
| Gold A | zone_demand_trend    | ~2.3M            | GROUP BY zone × date × hour          |
| Gold B | hourly_fare_profile  | ~336 rows        | JOIN + GROUP BY cab × dow × hour     |
| Gold C | driver_revenue_rank  | ~50K             | RANK() OVER (PARTITION BY trip_date) |
 
> Bronze is now a single unified table (`bronze.raw_trips`) with a `cab_type` column,
> replacing the previous per-vehicle-class tables.
 
---
 
## Dagster jobs & triggers
 
| Job | Trigger | Mode | Assets |
|---|---|---|---|
| `taxi_full_pipeline` | `daily_taxi_pipeline` cron (03:00 UTC) | Batch | `raw_trips_batch → clean_trips → gold` |
| `taxi_full_pipeline_kafka` | `tlc_new_file_sensor` (every 6 h) | Streaming queue | `download_trips → raw_trips → clean_trips → gold` |
 
The sensor performs a cheap `HEAD` request against TLC CloudFront for the last 3 months × 3 cab types. It tracks processed files in a JSON cursor and uses `run_key` deduplication so each `(cab, year, month)` combination is never processed twice.
 
---
 
## Quickstart
 
```bash
# 1. Start all services (Postgres, Kafka, Zookeeper, Dagster)
docker compose up --build
 
# 2. Open Dagster UI
open http://localhost:3000
 
# 3. Enable the sensor (UI → Automation → tlc_new_file_sensor → Enable)
#    The sensor will auto-detect and trigger runs for new TLC files.
#    To run the batch pipeline manually, launch taxi_full_pipeline from the UI.
 
# Connect to Postgres manually
docker exec -it postgres_db psql -U postgres taxidb
```
 
Expected runtime: ~30 min per year of data (batch mode)
 
---
 
## Kafka operations
 
```bash
# List topics and partition offsets (verify messages are present)
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic tlc_raw_files
 
# Check consumer group lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group bronze_ingest --describe
 
# Clear all messages (delete + recreate topic)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic tlc_raw_files
 
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic tlc_raw_files --partitions 3 --replication-factor 1
 
# Reset consumer group offsets (pipeline must be stopped first)
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group bronze_ingest --reset-offsets --to-earliest \
  --topic tlc_raw_files --execute
 
# Reset Dagster sensor cursor (re-detect all files on next tick)
docker exec dagster-daemon dagster sensor cursor set \
  --sensor-name tlc_new_file_sensor \
  --cursor '{"seen": []}' \
  -m pipeline.assets
```
 
---
 
## Environment variables
 
| Variable | Default | Description |
|---|---|---|
| `TLC_BASE` | CloudFront URL | Base URL for TLC parquet files |
| `DATA_DIR` | `/tmp/taxi_data` | Local mount for downloaded parquet files |
| `PG_URL` | `jdbc:postgresql://...` | Spark JDBC connection string |
| `PGUSER` / `PGPASSWORD` | `postgres` | PostgreSQL credentials |
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `tlc_raw_files` | Topic for file-pointer messages |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `SPARK_DRIVER_MEM` | `4g` | Spark driver memory |
 
---
 
## Key columns & derivations
 
| Column | Layer | Derivation |
|---|---|---|
| `cab_type` | Bronze | Literal injected at ingest (`yellow` / `green` / `fhv`) |
| `trip_duration_min` | Silver | `(dropoff_at - pickup_at)` in minutes |
| `avg_tip_pct` | Gold A | `tip_amount / fare_amount` averaged per zone-hour |
| `surge_index` | Gold B | `slot_avg_fare / cab_type_baseline_fare` |
| `revenue_rank` | Gold C | `RANK() OVER (PARTITION BY trip_date ORDER BY trip_count DESC)` |
 
---
 
## Silver filter criteria
 
Rows excluded from `clean_trips` if any apply:
- `fare_amount`, `trip_distance`, or `total_amount` ≤ 0
- `dropoff_at` ≤ `pickup_at`
- Trip duration outside 1–300 minutes
- Pickup or dropoff zone ID outside 1–263
- Pickup year outside 2019–2024
 