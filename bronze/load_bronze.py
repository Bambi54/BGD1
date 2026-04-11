import os, time, psycopg2, duckdb, numpy, pandas

DB = dict(
    host     = os.getenv("PGHOST",     "localhost"),
    port     = int(os.getenv("PGPORT", "5432")),
    dbname   = os.getenv("PGDATABASE", "taxidb"),
    user     = os.getenv("PGUSER",     "postgres"),
    password = os.getenv("PGPASSWORD", "postgres"),
)

def run_sql_file(conn: psycopg2.extensions.connection, path: str) -> None:
    with open(path, "r") as f:
        script = f.read()
    statements = [s.strip() for s in script.split(";") if s.strip()]
    with conn.cursor() as cur:
        for stmt in statements:
            lines = [l for l in stmt.splitlines() if not l.strip().startswith("--")]
            clean = "\n".join(lines).strip()
            if not clean:
                continue
            try:
                cur.execute(clean)
            except psycopg2.Error as e:
                print(f"[warn] {e.pgcode}: {str(e.pgerror)[:100].strip()}")
                conn.rollback()
    conn.commit()

START_YEAR = int(os.getenv("START_YEAR", "2023"))
END_YEAR   = int(os.getenv("END_YEAR",   "2023"))

PG_CONN_STR = (
    f"host={DB['host']} port={DB['port']} dbname={DB['dbname']} "
    f"user={DB['user']} password={DB['password']}"
)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

TABLE_SCHEMAS = {
    "yellow": [
        "vendorid",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "ratecodeid",
        "store_and_fwd_flag",
        "pulocationid",
        "dolocationid",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
    ],
    "green": [
        "vendorid",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "store_and_fwd_flag",
        "ratecodeid",
        "pulocationid",
        "dolocationid",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "ehail_fee",
        "improvement_surcharge",
        "total_amount",
        "payment_type",
        "trip_type",
        "congestion_surcharge",
    ],
    "fhv": [
        "dispatching_base_num",
        "pickup_datetime",
        "dropoff_datetime",
        "pulocationid",
        "dolocationid",
        "sr_flag",
        "affiliated_base_number",
    ],
}

CAB_TYPES = {
    "yellow": "yellow_tripdata",
    "green":  "green_tripdata",
    "fhv":    "fhv_tripdata",
}

SQL_PATH = "bronze/init_bronze.sql"


def months_in_range(start_year: int, end_year: int) -> list[tuple[int, int]]:
    months = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            months.append((year, month))
    return months


def parquet_url(cab_key: str, year: int, month: int) -> str:
    prefix = CAB_TYPES[cab_key]
    return f"{BASE_URL}/{prefix}_{year}-{month:02d}.parquet"


def load_parquet_to_bronze(
    duck: duckdb.DuckDBPyConnection,
    cab_key: str,
    year: int,
    month: int,
) -> int:
    url = parquet_url(cab_key, year, month)
    table = f"bronze.raw_{cab_key}_trips"
    target_cols = TABLE_SCHEMAS[cab_key]

    print(f"{cab_key} {year}-{month:02d}  {url}")
    t0 = time.time()

    try:
        cols_str  = ",".join(target_cols)

        rows_affected = duck.execute(f"""
            INSERT INTO {table} ({cols_str})
            SELECT {cols_str}
            FROM read_parquet('{url}')
        """).fetchone()[0]
        elapsed = time.time() - t0
        print(f"loaded {rows_affected} rows in {elapsed:.1f}s")
        return rows_affected
    except Exception as e:
        print(f"[skip] {e}")
        return 0


def main():
    print(f"Connecting to PostgreSQL at {DB['host']}:{DB['port']}...")
    pg_conn = psycopg2.connect(**DB)
    pg_conn.autocommit = False

    # print("Creating schemas and Bronze tables...")
    # run_sql_file(pg_conn, SQL_PATH)

    print("\nOpening DuckDB with postgres_scanner...")
    duck = duckdb.connect()
    duck.execute("INSTALL postgres; LOAD postgres;")
    duck.execute(f"ATTACH '{PG_CONN_STR}' AS pgdb (TYPE postgres);")
    duck.execute("USE pgdb;")

    months = months_in_range(START_YEAR, END_YEAR)
    print(f"\nLoading {len(months)} months × 3 cab types into Bronze...")

    for year, month in months:
        for cab_key in CAB_TYPES:
            load_parquet_to_bronze(duck, cab_key, year, month)

    duck.close()
    
    pg_conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()