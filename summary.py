import psycopg2

def print_summary(conn: psycopg2.extensions.connection) -> None:
    tables = [
        ("bronze.raw_yellow_trips",    "Bronze  raw_yellow_trips"),
        ("bronze.raw_green_trips",     "Bronze  raw_green_trips"),
        ("bronze.raw_fhv_trips",       "Bronze  raw_fhv_trips"),
        ("silver.clean_trips",         "Silver  clean_trips"),
        ("gold.zone_demand_trend",     "Gold A  zone_demand_trend"),
        ("gold.hourly_fare_profile",   "Gold B  hourly_fare_profile"),
        ("gold.driver_revenue_rank",   "Gold C  driver_revenue_rank"),
    ]
    print("\n── Row counts ──────────────────────────────────────────")
    with conn.cursor() as cur:
        for tbl, label in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                n = cur.fetchone()[0]
                print(f"{label:<42}: {n:>14,}")
            except Exception:
                print(f"{label:<42}: {'(not found)':>14}")

    print("\n── Gold B preview — surge index by hour (yellow, Friday) ─")
    with conn.cursor() as cur:
        try:
            cur.execute("""
                SELECT hour_of_day, avg_fare, surge_index, trip_count
                FROM gold.hourly_fare_profile
                WHERE cab_type = 'yellow' AND day_of_week = 5
                ORDER BY surge_index DESC
                LIMIT 8
            """)
            rows = cur.fetchall()
            print(f"{'hour':>5} {'avg_fare':>10} {'surge_idx':>10} {'trips':>10}")
            for r in rows:
                print(f"{r[0]:>5} {str(r[1]):>10} {str(r[2]):>10} {str(r[3]):>10}")
        except Exception as e:
            print(f"(query failed: {e})")



if __name__ == "__main__":
    print(f"Connecting to PostgreSQL at {DB['host']}:{DB['port']}...")
    pg_conn = psycopg2.connect(**DB)

    print_summary(pg_conn)
    pg_conn.close()