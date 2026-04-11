import psycopg2, os, time

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

SQL_PATH = "silver/load_silver.sql"

def main():
    pg_conn = psycopg2.connect(**DB)
    pg_conn.autocommit = False
    
    t0 = time.time()
    print("\nRunning Silver transformation...")
    run_sql_file(pg_conn, SQL_PATH)

    pg_conn.close()
    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")


if __name__ == "__main__":
    main()