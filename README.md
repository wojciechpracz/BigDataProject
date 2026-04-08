# BGD - Running Two Flows

## Requirements
- Docker + Docker Compose
- Python 3.10+
- Java (JDK 17+) for PySpark

Quick JDK installation on macOS (Homebrew):

```bash
brew install openjdk@17
java -version
echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@17"' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

## 1) Start Infrastructure
From the project root directory:

```bash
docker compose up -d
```

This starts:
- PostgreSQL (target layer for SQL baseline and Spark Silver)
- Minio (Bronze)
- Prefect Server (UI/API)

Prefect UI: http://localhost:4200
Minio Console: http://localhost:9001

## 2) Install Python Dependencies

```bash
pip install -r requirements.txt
```

## 3) Flow A - Baseline SQL ELT
Run baseline SQL manually with the orchestration script:

```bash
python 01_baseline_sql_elt/scripts/run_all.py
```

The script executes SQL files from `01_baseline_sql_elt/sql` in sequence and performs a quick table verification.

## 4) Flow B - Orchestrated Spark ELT
Run the Prefect flow (upload to Minio + Spark processing):

```bash
python 02_orchestrated_spark_elt/prefect_flow.py
```

The flow reads data from the `dataset` folder, uploads it to the `bronze` bucket, cleans the data, and writes `silver_cleaned_data` to PostgreSQL.

Architecture diagram:
- [Orchestrated Spark ELT Pipeline Architecture](02_orchestrated_spark_elt/docs/pipeline_architecture.md)

## 5) Stop the Environment

```bash
docker compose down
```
