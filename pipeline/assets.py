import os, requests, psycopg2, json
from datetime import date, timedelta
from dagster import (
    asset,
    sensor,
    SensorResult,
    RunRequest,
    SensorEvaluationContext,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    Definitions,
    ScheduleDefinition,
    RunConfig,
    define_asset_job,
    Config,
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, TimestampType
from pyspark.sql.window import Window
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException


class IngestConfig(Config):
    years:  list[int] = [2023]
    months: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]


TLC_BASE = os.getenv("TLC_BASE", "https://d37ci6vzurychx.cloudfront.net/trip-data")
DATA_DIR = os.getenv("DATA_DIR", "/tmp/taxi_data")
PG_URL = os.getenv("PG_URL",  "jdbc:postgresql://localhost:5432/taxidb")
PG_PROPS    = {"user": os.getenv("PGUSER", "postgres"),
               "password": os.getenv("PGPASSWORD", "postgres"),
               "driver": "org.postgresql.Driver"}

PG_CONFIG = {
    "host": os.getenv("PGHOST", "postgres"),
    "port": os.getenv("PGPORT", "5432"),
    "dbname": os.getenv("PGDATABASE", "taxidb"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
}

CAB_TYPES = {
    "yellow": "yellow_tripdata",
    "green":  "green_tripdata",
    "fhv":    "fhv_tripdata",
}

GOLD_A_SQL_PATH = "pipeline/sql/load_gold_a.sql"
GOLD_B_SQL_PATH = "pipeline/sql/load_gold_b.sql"
GOLD_C_SQL_PATH = "pipeline/sql/load_gold_c.sql"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",     "tlc_raw_files")

def run_sql_file(path: str):
    with open(path, "r") as f:
        return f.read()
    

def get_spark(app_name: str = "nyc_taxi_pipeline") -> SparkSession:
    return (
        SparkSession.builder
            .appName(app_name)
            .master(os.getenv("SPARK_MASTER", "local[*]"))
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.memory.fraction", "0.6")
            .config("spark.memory.storageFraction", "0.3")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEM", "4g"))
            .config("spark.executor.memory", "6g")
            .config("spark.executor.cores", "2")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
    )


def parquet_url(cab_key: str, year: int, month: int) -> str:
    prefix = CAB_TYPES[cab_key]
    return f"{TLC_BASE}/{prefix}_{year}-{month:02d}.parquet"


def _kafka_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def _kafka_consumer(group_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id":          group_id,
        "auto.offset.reset": "earliest",
        # Disable auto-commit so we only commit after a successful Spark write.
        "enable.auto.commit": False,
    })

def _publish_file_envelope(producer: Producer, cab: str, year: int,
                            month: int, local_path: str) -> None:
    """Publish a single file-pointer message to KAFKA_TOPIC."""
    payload = json.dumps({
        "cab":        cab,
        "year":       year,
        "month":      month,
        "local_path": local_path,
    }).encode()
    producer.produce(KAFKA_TOPIC, key=cab.encode(), value=payload)
                     

def download_file(url: str, local_path: str):
    if os.path.exists(local_path):
        return local_path

    r = requests.get(url, stream=True)

    if r.status_code == 200:
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return local_path


@asset(group_name="ingest", compute_kind="python")
def download_trips(context: AssetExecutionContext,
                   config: IngestConfig) -> MaterializeResult:
    os.makedirs(DATA_DIR, exist_ok=True)
 
    producer = _kafka_producer()
    published = 0
    skipped = 0
 
    for year in config.years:
        for month in config.months:
            for cab in CAB_TYPES:
                url = parquet_url(cab_key=cab, year=year, month=month)
                local_file = os.path.join(DATA_DIR, f"{cab}_{year}_{month:02d}.parquet")
 
                try:
                    download_file(url, local_file)
                    _publish_file_envelope(producer, cab, year, month, local_file)
                    published += 1
                    context.log.info(f"Published envelope: {cab} {year}-{month:02d}")
                except Exception as exc:
                    context.log.warning(f"Skipped {url}: {exc}")
                    skipped += 1
 
    producer.flush()
    context.log.info(f"Kafka flush complete. published={published} skipped={skipped}")
 
    return MaterializeResult(metadata={
        "published_messages": MetadataValue.int(published),
        "skipped":            MetadataValue.int(skipped),
        "kafka_topic":        MetadataValue.text(KAFKA_TOPIC),
    })
 

 
@asset(deps=[download_trips], group_name="bronze", compute_kind="pyspark")
def raw_trips(context: AssetExecutionContext,
              config: IngestConfig) -> MaterializeResult:

    consumer = _kafka_consumer(group_id="bronze_ingest")
    consumer.subscribe([KAFKA_TOPIC])
 
    spark = get_spark("bronze_ingest")
    mode = "overwrite"
    total_rows = 0
    processed = 0
 
    expected = len(config.years) * len(config.months) * len(CAB_TYPES)
    context.log.info(f"Expecting up to {expected} messages from Kafka")
 
    idle_polls = 0
    MAX_IDLE = 5
 
    while idle_polls < MAX_IDLE:
        msg = consumer.poll(timeout=10.0)
 
        if msg is None:
            idle_polls += 1
            context.log.debug(f"Empty poll ({idle_polls}/{MAX_IDLE})")
            continue
 
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                idle_polls += 1
                continue
            raise KafkaException(msg.error())
 
        idle_polls = 0
        envelope = json.loads(msg.value().decode())
        cab = envelope["cab"]
        year = envelope["year"]
        month = envelope["month"]
        local_path = envelope["local_path"]
 
        if not os.path.exists(local_path):
            context.log.warning(f"File missing on disk, skipping: {local_path}")
            consumer.commit(message=msg)
            continue
 
        try:
            df = spark.read.parquet(local_path)
            df = df.select(
                [F.col(c).cast("string").alias(c.lower().replace("lpep_", "").replace("tpep_", "").replace("id", "_id").replace(" ", "_"))
                    for c in df.columns]
            ).withColumn("cab_type", F.lit(cab)) \
            .withColumn("file_year", F.lit(int(year))) \
            .withColumn("file_month", F.lit(int(month)))
 
            row_count = df.count()
            df.write.mode("append").jdbc(PG_URL, "bronze.raw_trips", properties=PG_PROPS)
            mode = "append"
            total_rows += row_count
            processed  += 1
 
            consumer.commit(message=msg)
            context.log.info(
                f"Wrote {row_count} rows for {cab} {year}-{month:02d}"
            )
        except Exception as exc:
            context.log.error(
                f"Failed to write {cab} {year}-{month:02d}: {exc}. "
                "Offset NOT committed — will retry on next run."
            )
 
    consumer.close()
    spark.stop()
 
    return MaterializeResult(metadata={
        "row_count":   MetadataValue.int(total_rows),
        "files_loaded": MetadataValue.int(processed),
        "destination": MetadataValue.text("bronze.raw_trips"),
    })


@asset(group_name="bronze", compute_kind="pyspark")
def raw_trips_batch(context: AssetExecutionContext, config: IngestConfig) -> MaterializeResult:
    spark = get_spark("bronze_ingest")
    os.rmdir(DATA_DIR)
    os.makedirs(DATA_DIR, exist_ok=True)
 
    frames = []
    for year in config.years:
        for month in config.months:
            for cab in CAB_TYPES.keys():
                url = parquet_url(cab_key=cab, year=year, month=month)
                local_file = os.path.join(
                    DATA_DIR, f"{cab}_{year}_{month:02d}.parquet"
                )

                try:

                    download_file(url, local_file)
                    df = spark.read.parquet(local_file)
                    df = df.select(
                        [F.col(c).cast("string").alias(c.lower().replace("lpep_", "").replace("tpep_", "").replace("id", "_id").replace(" ", "_"))
                         for c in df.columns]
                    ).withColumn("cab_type", F.lit(cab)) \
                     .withColumn("file_year", F.lit(int(year))) \
                     .withColumn("file_month", F.lit(int(month)))
                    frames.append(df)
                    context.log.info(f"Loaded {url}")
                except Exception as e:
                    context.log.warning(f"Skipped {url}: {e}")
 
    if not frames:
        raise RuntimeError("No files loaded — check TLC_BASE and TAXI_YEARS")
 
    bronze = frames[0]
    for f in frames[1:]:
        bronze = bronze.unionByName(f, allowMissingColumns=True)
 
    row_count = bronze.count()
    bronze.write.mode("overwrite").jdbc(PG_URL, "bronze.raw_trips", properties=PG_PROPS)
 
    spark.stop()
    return MaterializeResult(metadata={
        "row_count":  MetadataValue.int(row_count),
        "destination": MetadataValue.text("bronze.raw_trips"),
    })


@asset(deps=[raw_trips], group_name="silver", compute_kind="pyspark")
def clean_trips(context: AssetExecutionContext, config: IngestConfig) -> MaterializeResult:
    spark = get_spark("silver_clean")

    context.log.info("Reading bronze table")
    mode = "overwrite"

    total_rows = 0
    for year in config.years:
        context.log.info(f"Processing year={year}")

        for month in config.months:
            context.log.info(f"Processing month={month}")

            query = f"""
            (
                SELECT *
                FROM bronze.raw_trips
                WHERE file_year = {year}
                AND file_month = {month}
            ) subq
            """

            df = spark.read.jdbc(
                url=PG_URL,
                table=query,
                properties=PG_PROPS
            )

            if df.rdd.isEmpty():
                context.log.warn("df is empty, skipping month")
                continue


            yellow = df.filter(F.col("cab_type") == "yellow") \
                .withColumn("pickup_at", F.to_timestamp("tpep_pickup_datetime")) \
                .withColumn("dropoff_at", F.to_timestamp("tpep_dropoff_datetime")) \
                .filter(
                    F.col("tpep_pickup_datetime").rlike(r"^\d{4}-\d{2}-\d{2}") &
                    F.col("tpep_dropoff_datetime").rlike(r"^\d{4}-\d{2}-\d{2}")
                ) \
                .withColumn("pickup_zone_id",
                    F.col("pulocationid").cast("double").cast(IntegerType())
                ) \
                .withColumn("dropoff_zone_id",
                    F.col("dolocationid").cast("double").cast(IntegerType())
                ) \
                .withColumn("trip_distance", F.col("trip_distance").cast(FloatType())) \
                .withColumn("fare_amount",   F.col("fare_amount").cast(FloatType())) \
                .withColumn("tip_amount",    F.col("tip_amount").cast(FloatType())) \
                .withColumn("total_amount",  F.col("total_amount").cast(FloatType())) \
                .withColumn("passenger_count",
                    F.col("passenger_count").cast("double").cast(IntegerType())
                ) \
                .withColumn("file_year", F.col("file_year")) \
                .withColumn("file_month", F.col("file_month")) \
                .withColumn("vendor_id", F.col("vendorid")) \
                .filter(
                    (F.col("fare_amount") > 0) &
                    (F.col("trip_distance") > 0) &
                    (F.col("total_amount") > 0)
                )


            green = df.filter(F.col("cab_type") == "green") \
                .withColumn("pickup_at", F.to_timestamp("lpep_pickup_datetime")) \
                .withColumn("dropoff_at", F.to_timestamp("lpep_dropoff_datetime")) \
                .filter(
                    F.col("lpep_pickup_datetime").rlike(r"^\d{4}-\d{2}-\d{2}") &
                    F.col("lpep_dropoff_datetime").rlike(r"^\d{4}-\d{2}-\d{2}")
                ) \
                .withColumn("pickup_zone_id",
                    F.col("pulocationid").cast("double").cast(IntegerType())
                ) \
                .withColumn("dropoff_zone_id",
                    F.col("dolocationid").cast("double").cast(IntegerType())
                ) \
                .withColumn("trip_distance", F.col("trip_distance").cast(FloatType())) \
                .withColumn("fare_amount",   F.col("fare_amount").cast(FloatType())) \
                .withColumn("tip_amount",    F.col("tip_amount").cast(FloatType())) \
                .withColumn("total_amount",  F.col("total_amount").cast(FloatType())) \
                .withColumn("passenger_count",
                    F.col("passenger_count").cast("double").cast(IntegerType())
                ) \
                .withColumn("file_year", F.col("file_year")) \
                .withColumn("file_month", F.col("file_month")) \
                .withColumn("vendor_id", F.col("vendorid")) \
                .filter(
                    (F.col("fare_amount") > 0) &
                    (F.col("trip_distance") > 0) &
                    (F.col("total_amount") > 0)
                )


            fhv = df.filter(F.col("cab_type") == "fhv") \
                .withColumn("dispatching_base_num", F.col("dispatching_base_num")) \
                .withColumn("pickup_at", F.to_timestamp("pickup_datetime")) \
                .withColumn("dropoff_at", F.to_timestamp("dropoff_datetime")) \
                .filter(
                    F.col("pickup_datetime").rlike(r"^\d{4}-\d{2}-\d{2}") &
                    F.col("dropoff_datetime").rlike(r"^\d{4}-\d{2}-\d{2}")
                ) \
                .withColumn("pickup_zone_id",
                    F.col("pulocationid").cast("double").cast(IntegerType())
                ) \
                .withColumn("dropoff_zone_id",
                    F.col("dolocationid").cast("double").cast(IntegerType())
                ) \
                .withColumn("trip_distance",   F.lit(None).cast(FloatType())) \
                .withColumn("fare_amount",     F.lit(None).cast(FloatType())) \
                .withColumn("tip_amount",      F.lit(None).cast(FloatType())) \
                .withColumn("total_amount",    F.lit(None).cast(FloatType())) \
                .withColumn("passenger_count", F.lit(None).cast(IntegerType())) \
                .withColumn("file_year", F.col("file_year")) \
                .withColumn("file_month", F.col("file_month")) \
                .withColumn("vendor_id", F.col("vendorid")) \


            combined = yellow.unionByName(green, allowMissingColumns=True) \
                            .unionByName(fhv, allowMissingColumns=True)


            combined = combined \
                .withColumn(
                    "trip_duration_min",
                    ((F.col("dropoff_at").cast("long") - F.col("pickup_at").cast("long")) / 60.0)
                ) \
                .filter(
                    (F.col("pickup_at") >= "2019-01-01") &
                    (F.col("pickup_at") < "2025-01-01") &
                    (F.col("dropoff_at") > F.col("pickup_at")) &
                    F.col("trip_duration_min").between(1, 300) &
                    F.col("pickup_zone_id").between(1, 263) &
                    F.col("dropoff_zone_id").between(1, 263)
                )


            w = Window.partitionBy(
                "cab_type", "pickup_at", "dropoff_at", "pickup_zone_id"
            ).orderBy(F.lit(1))

            combined = combined.withColumn("rn", F.row_number().over(w)) \
                            .filter("rn = 1") \
                            .drop("rn")


            combined = combined \
                .select(
                    "cab_type",
                    "pickup_at",
                    "dropoff_at",
                    "pickup_zone_id",
                    "dropoff_zone_id",
                    "trip_distance",
                    "fare_amount",
                    "tip_amount",
                    "total_amount",
                    "passenger_count",
                    "trip_duration_min",
                    "dispatching_base_num",
                    "file_year",
                    "file_month",
                    "vendor_id"
                ) \
                .withColumn("trip_id", F.monotonically_increasing_id()) \
                .withColumn("loaded_at", F.current_timestamp())

            combined.write \
                .mode(mode) \
                .option("batchsize", 10000) \
                .jdbc(PG_URL, "silver.clean_trips", properties=PG_PROPS)

            mode = "append"

            count = combined.count()
            total_rows += count

            context.log.info(f"Wrote {count} rows")

    spark.stop()

 
    return MaterializeResult(metadata={
        "total_rows": MetadataValue.int(total_rows),
        "destination": MetadataValue.text("silver.clean_trips"),
    })


def run_sql(context: AssetExecutionContext, sql_path: str, layer: str):
    context.log.info("Connecting to Postgres...")

    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = True

    sql_script = run_sql_file(sql_path)

    try:
        with conn.cursor() as cur:
            context.log.info(f"Executing Gold Layer {layer} SQL")
            cur.execute(sql_script)

        context.log.info("SQL executed successfully")

    except Exception as e:
        context.log.error(f"SQL execution failed: {str(e)}")
        raise

    finally:
        conn.close()


@asset(deps=[clean_trips], group_name="gold", compute_kind="pyspark")
def zone_demand_trend(context: AssetExecutionContext) -> MaterializeResult:
    run_sql(context, GOLD_A_SQL_PATH, 'A')

    return MaterializeResult(
        metadata={
            "status": MetadataValue.text("success"),
            "sql_file": MetadataValue.text(GOLD_A_SQL_PATH),
        }
    )


@asset(deps=[clean_trips], group_name="gold", compute_kind="sql")
def hourly_fare_profile(context: AssetExecutionContext):
    run_sql(context, GOLD_B_SQL_PATH, 'B')

    return MaterializeResult(
        metadata={
            "table": MetadataValue.text("gold.hourly_fare_profile"),
            "sql": MetadataValue.text(GOLD_B_SQL_PATH),
        }
    )


@asset(deps=[clean_trips], group_name="gold", compute_kind="sql")
def driver_revenue_rank(context: AssetExecutionContext):
    run_sql(context, GOLD_C_SQL_PATH, 'C')

    return MaterializeResult(
        metadata={
            "table": MetadataValue.text("gold.driver_revenue_rank"),
            "sql": MetadataValue.text(GOLD_C_SQL_PATH),
        }
    )


taxi_pipeline_job_batch = define_asset_job(
    name="taxi_full_pipeline",
    selection=[
        raw_trips_batch,
        clean_trips,
        zone_demand_trend,
        hourly_fare_profile,
        driver_revenue_rank,
    ],
    config=RunConfig(
        ops={"raw_trips_batch": IngestConfig(years=[2023], months=list(range(1, 13)))}
    ),
)
 
daily_schedule = ScheduleDefinition(
    job=taxi_pipeline_job_batch,
    cron_schedule="0 3 * * *",   # every day at 03:00
    name="daily_taxi_pipeline",
)

taxi_pipeline_job_kafka = define_asset_job(
    name="taxi_full_pipeline_kafka",
    selection=[
        download_trips,
        raw_trips,
        clean_trips,
        zone_demand_trend,
        hourly_fare_profile,
        driver_revenue_rank,
    ],
    config=RunConfig(
        ops={"download_trips": IngestConfig(years=[2026], months=list(range(1, 3)))}
    ),
)

def _tlc_file_exists(cab: str, year: int, month: int) -> bool:
    """HEAD request — cheap check, no download."""
    url = parquet_url(cab_key=cab, year=year, month=month)
    try:
        r = requests.head(url, timeout=10)
        return r.status_code == 200
    except requests.RequestException:
        return False
 
 
@sensor(
    job=taxi_pipeline_job_kafka,
    minimum_interval_seconds=6 * 3600,
    name="tlc_new_file_sensor",
    description=(
        "Polls TLC CloudFront for newly published monthly parquet files. "
        "Fires taxi_full_pipeline_kafka automatically when a new month is detected."
    ),
)
def tlc_new_file_sensor(context: SensorEvaluationContext): 
    cursor_data: dict = json.loads(context.cursor or '{"seen": []}')
    seen: set = set(cursor_data.get("seen", []))
    new_seen = set(seen)
 
    run_requests = []
 
    today = date.today()
    months_to_check = []

    for delta_months in range(3):
        year  = today.year  - ((today.month - 1 - delta_months) < 0)
        month = ((today.month - 1 - delta_months) % 12) + 1
        months_to_check.append((year, month))
 
    for year, month in months_to_check:
        for cab in CAB_TYPES:
            key = f"{cab}_{year}_{month:02d}"
            if key in seen:
                continue
 
            if _tlc_file_exists(cab, year, month):
                context.log.info(f"New TLC file detected: {key} — triggering run")
                run_requests.append(
                    RunRequest(
                        run_key=key,
                        run_config=RunConfig(
                            ops={
                                "download_trips": IngestConfig(
                                    years=[year],
                                    months=[month],
                                )
                            }
                        ),
                        tags={"cab": cab, "year": str(year), "month": str(month)},
                    )
                )
                new_seen.add(key)
 
    new_cursor = json.dumps({"seen": sorted(new_seen)})
 
    return SensorResult(
        run_requests=run_requests,
        cursor=new_cursor,
    )
 

defs = Definitions(
    assets=[
        download_trips,
        raw_trips,
        raw_trips_batch,
        clean_trips,
        zone_demand_trend,
        hourly_fare_profile,
        driver_revenue_rank,
    ],
    jobs=[taxi_pipeline_job_batch, taxi_pipeline_job_kafka],
    schedules=[daily_schedule],
    sensors=[tlc_new_file_sensor]
)