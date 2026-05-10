"""
spark_processing.py – three-stage Spark pipeline:

  Stage 1  kafka_to_bronze  – Spark Structured Streaming reads the three
           Kafka topics (flights, airlines, airports) and lands raw Parquet
           files in MinIO Bronze using trigger(availableNow=True).

  Stage 2  bronze_to_silver – Spark batch job joins the three Bronze datasets,
           cleans the result, and writes it to PostgreSQL Silver
           (table: silver_cleaned_data).

  Stage 3  silver_to_gold   – Spark batch job reads Silver and computes
           three analytical aggregations stored as Gold tables in PostgreSQL:
             • gold_airline_performance
             • gold_airport_delay_summary
             • gold_delay_by_month_weekday

All stages share a single SparkSession built by run_spark_pipeline().
"""

import shutil
import tempfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    from_json,
    lit,
    round as spark_round,
    sum as spark_sum,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Kafka / MinIO / PostgreSQL connection constants
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = "localhost:9094"
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin_password"
PG_URL = "jdbc:postgresql://localhost:5432/medallion_db"
PG_USER = "admin"
PG_PASSWORD = "admin_password"

# ---------------------------------------------------------------------------
# Explicit schemas for each Kafka topic
# ---------------------------------------------------------------------------

FLIGHTS_SCHEMA = StructType(
    [
        StructField("YEAR", IntegerType()),
        StructField("MONTH", IntegerType()),
        StructField("DAY", IntegerType()),
        StructField("DAY_OF_WEEK", IntegerType()),
        StructField("AIRLINE", StringType()),
        StructField("FLIGHT_NUMBER", StringType()),
        StructField("TAIL_NUMBER", StringType()),
        StructField("ORIGIN_AIRPORT", StringType()),
        StructField("DESTINATION_AIRPORT", StringType()),
        StructField("SCHEDULED_DEPARTURE", StringType()),
        StructField("DEPARTURE_TIME", StringType()),
        StructField("DEPARTURE_DELAY", DoubleType()),
        StructField("TAXI_OUT", DoubleType()),
        StructField("WHEELS_OFF", StringType()),
        StructField("SCHEDULED_TIME", DoubleType()),
        StructField("ELAPSED_TIME", DoubleType()),
        StructField("AIR_TIME", DoubleType()),
        StructField("DISTANCE", DoubleType()),
        StructField("WHEELS_ON", StringType()),
        StructField("TAXI_IN", DoubleType()),
        StructField("SCHEDULED_ARRIVAL", StringType()),
        StructField("ARRIVAL_TIME", StringType()),
        StructField("ARRIVAL_DELAY", DoubleType()),
        StructField("DIVERTED", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("CANCELLATION_REASON", StringType()),
        StructField("AIR_SYSTEM_DELAY", DoubleType()),
        StructField("SECURITY_DELAY", DoubleType()),
        StructField("AIRLINE_DELAY", DoubleType()),
        StructField("LATE_AIRCRAFT_DELAY", DoubleType()),
        StructField("WEATHER_DELAY", DoubleType()),
    ]
)

AIRLINES_SCHEMA = StructType(
    [
        StructField("IATA_CODE", StringType()),
        StructField("AIRLINE", StringType()),
    ]
)

AIRPORTS_SCHEMA = StructType(
    [
        StructField("IATA_CODE", StringType()),
        StructField("AIRPORT", StringType()),
        StructField("CITY", StringType()),
        StructField("STATE", StringType()),
        StructField("COUNTRY", StringType()),
        StructField("LATITUDE", DoubleType()),
        StructField("LONGITUDE", DoubleType()),
    ]
)


# ---------------------------------------------------------------------------
# Stage 1: Kafka → Bronze (Spark Structured Streaming)
# ---------------------------------------------------------------------------


def kafka_to_bronze(spark: SparkSession, checkpoint_base: str) -> None:
    """
    Read all three Kafka topics as a single multi-topic stream and write each
    topic's data to a separate Parquet path in MinIO Bronze.

    trigger(availableNow=True) processes all currently available messages in a
    bounded run (batch-friendly) and then terminates each streaming query.
    """

    def _stream_topic(
        topic: str,
        schema: StructType,
        bronze_path: str,
    ) -> None:
        stream_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            # Read from the very beginning each time (idempotent re-runs).
            .option("startingOffsets", "earliest")
            # Prevents failures when the topic contains more data than expected.
            .option("failOnDataLoss", "false")
            .load()
        )

        # Kafka delivers the payload in the binary `value` column.
        # Parse it as JSON using the known schema.
        parsed_df = stream_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        query = (
            parsed_df.writeStream.format("parquet")
            .option("path", bronze_path)
            .option("checkpointLocation", f"{checkpoint_base}/{topic}")
            # availableNow: consume all offsets present at query start, then stop.
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()

    _stream_topic("flights", FLIGHTS_SCHEMA, "s3a://bronze/flights/")
    _stream_topic("airlines", AIRLINES_SCHEMA, "s3a://bronze/airlines/")
    _stream_topic("airports", AIRPORTS_SCHEMA, "s3a://bronze/airports/")


# ---------------------------------------------------------------------------
# Stage 2: Bronze → Silver (Spark Batch)
# ---------------------------------------------------------------------------


def bronze_to_silver(spark: SparkSession) -> None:
    """
    Read the three Bronze Parquet datasets, join them, clean the result, and
    write to the PostgreSQL Silver table.

    Joins
    -----
    flights ⟕ airlines  on  flights.AIRLINE       = airlines.IATA_CODE
    result  ⟕ airports  on  flights.ORIGIN_AIRPORT = airports.IATA_CODE
    """

    flights_df: DataFrame = spark.read.parquet("s3a://bronze/flights/")
    airlines_df: DataFrame = spark.read.parquet("s3a://bronze/airlines/")
    airports_df: DataFrame = spark.read.parquet("s3a://bronze/airports/")

    # Disambiguate column names before joins.
    airlines_renamed = airlines_df.withColumnRenamed("IATA_CODE", "AIRLINE_CODE").withColumnRenamed(
        "AIRLINE", "AIRLINE_NAME"
    )
    airports_renamed = (
        airports_df.withColumnRenamed("IATA_CODE", "AIRPORT_CODE")
        .withColumnRenamed("AIRPORT", "ORIGIN_AIRPORT_NAME")
        .withColumnRenamed("CITY", "ORIGIN_CITY")
        .withColumnRenamed("STATE", "ORIGIN_STATE")
        .withColumnRenamed("COUNTRY", "ORIGIN_COUNTRY")
        .withColumnRenamed("LATITUDE", "ORIGIN_LATITUDE")
        .withColumnRenamed("LONGITUDE", "ORIGIN_LONGITUDE")
    )

    joined_df = (
        flights_df.join(
            airlines_renamed,
            flights_df["AIRLINE"] == airlines_renamed["AIRLINE_CODE"],
            how="left",
        )
        .join(
            airports_renamed,
            flights_df["ORIGIN_AIRPORT"] == airports_renamed["AIRPORT_CODE"],
            how="left",
        )
        .drop("AIRLINE_CODE", "AIRPORT_CODE")
    )

    silver_df = joined_df.dropDuplicates().dropna()

    (
        silver_df.write.format("jdbc")
        .option("url", PG_URL)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "silver_cleaned_data")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        # Overwrite keeps the run idempotent by recreating the target table each run.
        .mode("overwrite")
        .save()
    )


# ---------------------------------------------------------------------------
# Stage 3: Silver → Gold (Spark Batch)
# ---------------------------------------------------------------------------


def _write_gold_table(df: DataFrame, table: str) -> None:
    """Write a Gold DataFrame to a PostgreSQL table (overwrite, idempotent)."""
    (
        df.write.format("jdbc")
        .option("url", PG_URL)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", table)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .mode("overwrite")
        .save()
    )


def silver_to_gold(spark: SparkSession) -> None:
    """
    Read Silver (PostgreSQL) and produce three Gold aggregation tables.

    gold_airline_performance
        Per-airline KPIs: total/cancelled/operated flights, avg delays, on-time rate.

    gold_airport_delay_summary
        Per-origin-airport KPIs: total departures, avg delays, cancellation rate.

    gold_delay_by_month_weekday
        Heatmap grain (month × day-of-week): avg delays, cancellation rate.
    """
    silver_df = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "silver_cleaned_data")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .load()
        # Cache because all three aggregations read from the same table.
        .cache()
    )

    cancelled = col("CANCELLED") == 1
    operated = col("CANCELLED") == 0

    # ------------------------------------------------------------------
    # 1. gold_airline_performance
    # ------------------------------------------------------------------
    airline_perf = silver_df.groupBy("AIRLINE", "AIRLINE_NAME").agg(
        spark_sum(lit(1)).alias("total_flights"),
        spark_sum(when(cancelled, 1).otherwise(0)).alias("cancelled_flights"),
        spark_round(
            100.0 * spark_sum(when(cancelled, 1).otherwise(0)) / spark_sum(lit(1)), 2
        ).alias("cancellation_rate_pct"),
        spark_sum(when(operated, 1).otherwise(0)).alias("operated_flights"),
        spark_round(avg(when(operated, col("ARRIVAL_DELAY"))), 2).alias("avg_arrival_delay_min"),
        spark_round(avg(when(operated, col("DEPARTURE_DELAY"))), 2).alias("avg_departure_delay_min"),
        spark_round(
            100.0
            * spark_sum(
                when(operated & (coalesce(col("ARRIVAL_DELAY"), lit(0.0)) <= 0), 1).otherwise(0)
            )
            / spark_sum(when(operated, 1).otherwise(lit(None))),
            2,
        ).alias("on_time_rate_pct"),
    )
    _write_gold_table(airline_perf, "gold_airline_performance")

    # ------------------------------------------------------------------
    # 2. gold_airport_delay_summary
    # ------------------------------------------------------------------
    airport_summary = silver_df.groupBy(
        "ORIGIN_AIRPORT",
        "ORIGIN_AIRPORT_NAME",
        "ORIGIN_CITY",
        "ORIGIN_STATE",
    ).agg(
        spark_sum(lit(1)).alias("total_departures"),
        spark_round(avg(when(operated, col("DEPARTURE_DELAY"))), 2).alias("avg_departure_delay_min"),
        spark_round(avg(when(operated, col("ARRIVAL_DELAY"))), 2).alias("avg_arrival_delay_min"),
        spark_sum(when(cancelled, 1).otherwise(0)).alias("cancelled_flights"),
        spark_round(
            100.0 * spark_sum(when(cancelled, 1).otherwise(0)) / spark_sum(lit(1)), 2
        ).alias("cancellation_rate_pct"),
    )
    _write_gold_table(airport_summary, "gold_airport_delay_summary")

    # ------------------------------------------------------------------
    # 3. gold_delay_by_month_weekday
    # ------------------------------------------------------------------
    month_weekday = silver_df.groupBy("MONTH", "DAY_OF_WEEK").agg(
        spark_sum(lit(1)).alias("total_flights"),
        spark_round(avg(when(operated, col("ARRIVAL_DELAY"))), 2).alias("avg_arrival_delay_min"),
        spark_round(avg(when(operated, col("DEPARTURE_DELAY"))), 2).alias("avg_departure_delay_min"),
        spark_round(
            100.0 * spark_sum(when(cancelled, 1).otherwise(0)) / spark_sum(lit(1)), 2
        ).alias("cancellation_rate_pct"),
    )
    _write_gold_table(month_weekday, "gold_delay_by_month_weekday")

    silver_df.unpersist()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_spark_pipeline() -> None:
    """Build a SparkSession and run both pipeline stages sequentially."""
    # PySpark requires a local Java runtime; fail early with a clear message.
    if shutil.which("java") is None:
        raise EnvironmentError(
            "The 'java' command was not found. Install a JDK (for example temurin@17) "
            "and run the pipeline again."
        )

    spark = (
        SparkSession.builder.appName("orchestrated_spark_elt")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            # S3A / MinIO support
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            # Kafka connector for Structured Streaming
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            # PostgreSQL JDBC driver
            "org.postgresql:postgresql:42.7.3",
        )
        # MinIO (S3-compatible) settings
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    # Use a temporary directory for streaming checkpoints so re-runs always
    # replay from earliest offsets (stateless, idempotent behaviour).
    checkpoint_base = str(Path(tempfile.mkdtemp()) / "checkpoints")

    try:
        kafka_to_bronze(spark, checkpoint_base)
        bronze_to_silver(spark)
        silver_to_gold(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    run_spark_pipeline()
