"""
kafka_producer.py – reads the three source CSV files and publishes every row
as a JSON message to the matching Kafka topic.

Topics  : flights  (partitions=3, key=FLIGHT_NUMBER)
          airlines (partitions=1)
          airports (partitions=1)

Bootstrap: localhost:9094  (external listener defined in docker-compose.yml)
"""

import csv
import json
import logging
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BOOTSTRAP_SERVERS = ["localhost:9094"]

DATASET_DIR = Path(__file__).resolve().parents[1] / "dataset"

TOPIC_FILE_MAP: dict[str, Path] = {
    "flights": DATASET_DIR / "flights.csv",
    "airlines": DATASET_DIR / "airlines.csv",
    "airports": DATASET_DIR / "airports.csv",
}


# ---------------------------------------------------------------------------
# Producer helpers
# ---------------------------------------------------------------------------


def _build_producer() -> KafkaProducer:
    """Create a KafkaProducer with JSON serialisation."""
    try:
        return KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            # Deliver all replicas before ack (data safety).
            acks="all",
            # Retry transient send errors up to 3 times.
            retries=3,
        )
    except NoBrokersAvailable as exc:
        raise ConnectionError(
            f"Cannot reach Kafka at {BOOTSTRAP_SERVERS}. "
            "Make sure the broker is running and the external port 9094 is exposed."
        ) from exc


def _produce_topic(producer: KafkaProducer, topic: str, csv_path: Path) -> int:
    """Send every row in *csv_path* to *topic*. Returns the number of messages sent."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Source file not found: {csv_path}")

    count = 0
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            # Use FLIGHT_NUMBER as partition key for flights (better load distribution).
            key: bytes | None = None
            if topic == "flights" and "FLIGHT_NUMBER" in row:
                key = str(row["FLIGHT_NUMBER"]).encode("utf-8")

            producer.send(topic, key=key, value=dict(row))
            count += 1

    # Flush ensures all buffered messages are sent before returning.
    producer.flush()
    return count


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def produce_all() -> dict[str, int]:
    """
    Publish all three CSV datasets to their Kafka topics.

    Returns a mapping of topic name → number of messages produced.
    """
    producer = _build_producer()
    counts: dict[str, int] = {}
    try:
        for topic, csv_path in TOPIC_FILE_MAP.items():
            logger.info("Producing %s → topic '%s' …", csv_path.name, topic)
            n = _produce_topic(producer, topic, csv_path)
            counts[topic] = n
            logger.info("  %d messages sent to '%s'.", n, topic)
    finally:
        producer.close()

    return counts


# ---------------------------------------------------------------------------
# Stand-alone execution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = produce_all()
    for topic, n in result.items():
        print(f"  {topic:10s}: {n:,} messages")
