import shutil

from pyspark.sql import SparkSession


def run_spark_pipeline() -> None:
    """Runs a simple Bronze (Minio) -> Silver (PostgreSQL) pipeline."""
    # PySpark requires a local Java runtime (JDK/JRE), otherwise the gateway cannot start.
    if shutil.which("java") is None:
        raise EnvironmentError(
            "The 'java' command was not found. Install a JDK (for example temurin@17) "
            "and run the pipeline again."
        )

    spark = (
        SparkSession.builder.appName("orchestrated_spark_elt")
        .master("local[*]")
        # Packages: S3A support for Minio and JDBC driver for PostgreSQL.
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.7.3",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin_password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    try:
        bronze_df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            # S3A lets Spark read Minio objects as S3-compatible storage.
            .csv("s3a://bronze/dataset.csv")
        )

        silver_df = bronze_df.dropDuplicates().dropna()

        (
            silver_df.write.format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/medallion_db")
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", "silver_cleaned_data")
            .option("user", "admin")
            .option("password", "admin_password")
            # Overwrite keeps the run idempotent by recreating the target table each time.
            .mode("overwrite")
            .save()
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    run_spark_pipeline()
