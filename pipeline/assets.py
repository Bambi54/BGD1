import os, requests
from dagster import (
    asset,
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


class IngestConfig(Config):
    years:  list[int] = [2023]
    months: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]


TLC_BASE = os.getenv("TLC_BASE", "https://d37ci6vzurychx.cloudfront.net/trip-data")
DATA_DIR = os.getenv("DATA_DIR", "/tmp/taxi_data")
PG_URL = os.getenv("PG_URL",  "jdbc:postgresql://localhost:5432/taxidb")
PG_PROPS    = {"user": os.getenv("PGUSER", "postgres"),
               "password": os.getenv("PGPASSWORD", "postgres"),
               "driver": "org.postgresql.Driver"}

CAB_TYPES = {
    "yellow": "yellow_tripdata",
    "green":  "green_tripdata",
    "fhv":    "fhv_tripdata",
}

def get_spark(app_name: str = "nyc_taxi_pipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEM", "4g"))
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def parquet_url(cab_key: str, year: int, month: int) -> str:
    prefix = CAB_TYPES[cab_key]
    return f"{TLC_BASE}/{prefix}_{year}-{month:02d}.parquet"


@asset(group_name="bronze", compute_kind="pyspark")
def raw_trips(context: AssetExecutionContext, config: IngestConfig) -> MaterializeResult:
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

                    def download_file(url: str, local_path: str):
                        if os.path.exists(local_path):
                            return local_path

                        r = requests.get(url, stream=True)

                        if r.status_code == 200:
                            with open(local_path, "wb") as f:
                                for chunk in r.iter_content(chunk_size=8192):
                                    f.write(chunk)

                        return local_path
                    

                    download_file(url, local_file)
                    df = spark.read.parquet(local_file)
                    df = df.select(
                        [F.col(c).cast("string").alias(c.lower().replace(" ", "_"))
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


taxi_pipeline_job = define_asset_job(
    name="taxi_full_pipeline",
    selection="*",
    config=RunConfig(
        ops={"raw_trips": IngestConfig(years=[2023], months=list(range(1, 13)))}
    ),
)
 
daily_schedule = ScheduleDefinition(
    job=taxi_pipeline_job,
    cron_schedule="0 3 * * *",   # every day at 03:00
    name="daily_taxi_pipeline",
)
 
defs = Definitions(
    assets=[
        raw_trips,
    ],
    schedules=[daily_schedule],
)