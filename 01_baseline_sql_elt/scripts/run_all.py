import subprocess
import sys
from pathlib import Path

CONTAINER = "medallion_postgres"
DB = "medallion_db"
USER = "admin"

BASE_DIR = Path(__file__).resolve().parents[1]
SQL_DIR = BASE_DIR / "sql"

STEPS = [
    ("[1/4] Setting up schemas...", SQL_DIR / "01_setup.sql"),
    ("[2/4] Loading raw data (flights.csv is large)...", SQL_DIR / "02_raw_load.sql"),
    ("[3/4] Creating cleaned layer...", SQL_DIR / "03_cleaned.sql"),
    ("[4/4] Creating gold layer...", SQL_DIR / "04_gold.sql"),
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


def run_sql(step, filepath: Path):
    print()
    print("==========================================")
    print(f" {step}")
    print("==========================================")
    with open(filepath, "r", encoding="utf-8") as f:
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
    print(f"Or use psql:  docker exec -it {CONTAINER} psql -U {USER} -d {DB}")


if __name__ == "__main__":
    main()
