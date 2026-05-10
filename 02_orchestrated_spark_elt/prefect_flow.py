from kafka_producer import produce_all
from prefect import flow, get_run_logger, task

from spark_processing import run_spark_pipeline


@task(name="inject_to_kafka")
def inject_to_kafka() -> dict[str, int]:
    """Reads the three source CSV files and publishes every row to the matching Kafka topic."""
    logger = get_run_logger()
    logger.info("Starting Kafka ingestion …")
    counts = produce_all()
    for topic, n in counts.items():
        logger.info("  %-10s → %d messages produced.", topic, n)
    return counts


@task(name="run_data_processing")
def run_data_processing() -> None:
    """Runs the three-stage Spark pipeline: Kafka → Bronze (streaming), Bronze → Silver (batch), Silver → Gold (batch)."""
    run_spark_pipeline()


@flow(name="orchestrated_spark_elt_flow")
def orchestrated_spark_elt_flow() -> None:
    """
    Entry point.

    1. inject_to_kafka   – publish CSV rows to Kafka topics (flights, airlines, airports)
    2. run_data_processing:
         a. kafka_to_bronze  – Spark Structured Streaming lands raw Parquet in MinIO Bronze
         b. bronze_to_silver – Spark batch joins, cleans, and writes to PostgreSQL Silver
         c. silver_to_gold   – Spark batch computes Gold aggregations in PostgreSQL:
                                 gold_airline_performance
                                 gold_airport_delay_summary
                                 gold_delay_by_month_weekday
    """
    logger = get_run_logger()

    counts = inject_to_kafka()
    total = sum(counts.values())
    logger.info("Kafka ingestion complete — %d total messages across %d topics.", total, len(counts))

    run_data_processing()
    logger.info("ELT pipeline finished successfully.")


if __name__ == "__main__":
    orchestrated_spark_elt_flow()
