from pathlib import Path

from minio import Minio
from prefect import flow, get_run_logger, task

from spark_processing import run_spark_pipeline


@task(name="upload_to_minio")
def upload_to_minio() -> str:
    """Uploads the source file to the Bronze bucket in Minio."""
    client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="admin_password",
        secure=False,
    )

    bucket_name = "bronze"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    dataset_dir = Path(__file__).resolve().parents[1] / "dataset"
    # Prefer dataset.csv, but keep flights.csv as a fallback.
    source_file = dataset_dir / "dataset.csv"
    if not source_file.exists():
        source_file = dataset_dir / "flights.csv"

    if not source_file.exists():
        raise FileNotFoundError(
            "Missing dataset.csv and flights.csv in the dataset directory."
        )

    client.fput_object(bucket_name, "dataset.csv", str(source_file))

    return f"Uploaded {source_file.name} to bucket {bucket_name}."


@task(name="run_data_processing")
def run_data_processing() -> None:
    """Runs local Spark processing for the Silver layer."""
    run_spark_pipeline()


@flow(name="orchestrated_spark_elt_flow")
def orchestrated_spark_elt_flow() -> None:
    """Single entry point: upload to Bronze, then transform to Silver."""
    logger = get_run_logger()

    upload_message = upload_to_minio()
    logger.info(upload_message)

    run_data_processing()
    logger.info("ELT pipeline finished successfully.")


if __name__ == "__main__":
    orchestrated_spark_elt_flow()
