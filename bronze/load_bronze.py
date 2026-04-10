import os, time, psycopg2, duckdb

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

    print(f"{cab_key} {year}-{month:02d}  {url}")
    t0 = time.time()

    try:
        duck.execute(f"""
            INSERT INTO {table}
            SELECT *
            FROM read_parquet('{url}')
        """)
        duck.execute(f"SELECT changes()").fetchone()
        elapsed = time.time() - t0
        rows = duck.execute(f"""
            SELECT COUNT(*) FROM {table}
            WHERE tpep_pickup_datetime LIKE '{year}-{month:02d}%'
            OR lpep_pickup_datetime LIKE '{year}-{month:02d}%'
            OR pickup_datetime LIKE '{year}-{month:02d}%'
        """).fetchone()[0] if cab_key != "fhv" else 0
        print(f"loaded in {elapsed:.1f}s")
        return rows
    except Exception as e:
        print(f"[skip] {e}")
        return 0


def main():
    print(f"Connecting to PostgreSQL at {DB['host']}:{DB['port']}...")
    pg_conn = psycopg2.connect(**DB)
    pg_conn.autocommit = False

    print("Creating schemas and Bronze tables...")
    run_sql_file(pg_conn, SQL_PATH)

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