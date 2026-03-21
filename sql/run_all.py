import subprocess
import sys

CONTAINER = "bgd_postgres"
DB = "bgd_flights"
USER = "bgd"

STEPS = [
    ("[1/4] Setting up schemas...",                     "sql/01_setup.sql"),
    ("[2/4] Loading raw data (flights.csv is large)...", "sql/02_raw_load.sql"),
    ("[3/4] Creating cleaned layer...",                  "sql/03_cleaned.sql"),
    ("[4/4] Creating gold layer...",                     "sql/04_gold.sql"),
]

VERIFY_SQL = """\
SELECT schemaname, relname AS tablename, n_live_tup AS approx_rows
FROM pg_stat_user_tables
WHERE schemaname IN ('raw', 'cleaned', 'gold')
ORDER BY schemaname, relname;
"""


def check_container():
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        capture_output=True, text=True, check=True,
    )
    running = result.stdout.splitlines()
    if CONTAINER not in running:
        print(f"ERROR: Container '{CONTAINER}' is not running.")
        print("Start it first with:  docker compose up -d")
        sys.exit(1)


def run_sql(step, filepath):
    print()
    print("==========================================")
    print(f" {step}")
    print("==========================================")
    with open(filepath, "r") as f:
        subprocess.run(
            ["docker", "exec", "-i", CONTAINER, "psql", "-U", USER, "-d", DB],
            stdin=f, check=True,
        )


def main():
    check_container()

    for step, filepath in STEPS:
        run_sql(step, filepath)

    print()
    print("==========================================")
    print(" Pipeline complete. Quick verification:")
    print("==========================================")
    subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "psql", "-U", USER, "-d", DB],
        input=VERIFY_SQL, text=True, check=True,
    )

    print()
    print(f"Connect to pgAdmin: http://localhost:8080  (admin@bgd.com / admin)")
    print(f"Or use psql:  docker exec -it {CONTAINER} psql -U {USER} -d {DB}")


if __name__ == "__main__":
    main()
