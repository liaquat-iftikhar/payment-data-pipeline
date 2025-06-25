import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count
from typing import List

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class GamePaymentETL:
    """
    ETL job to process game payment transactions from raw JSON files into
    curated Parquet tables: raw data and aggregated revenue per game per day.

    Args:
        settings: A Dynaconf settings object injected from external config.
    """

    def __init__(self, settings):
        self.settings = settings
        self.spark = None
        self.bucket = settings.bucket_name
        self.start_date = settings.start_date
        self.end_date = settings.end_date
        self.input_prefix = f"s3a://{self.bucket}/raw/payments_{self.start_date}_{self.end_date}"
        self.output_raw_path = f"s3a://{self.bucket}/curated/payments_raw/"
        self.output_agg_path = f"s3a://{self.bucket}/enriched/payments_agg/"

    def _init_spark(self):
        """Initializes the SparkSession."""
        self.spark = SparkSession.builder.appName("GamePaymentETL").getOrCreate()

    def _load_file_paths_from_manifest(self) -> List[str]:
        """
        Loads the list of S3 keys from the manifest.json.

        Returns:
            A list of S3 URIs to JSON files containing payment data.

        Raises:
            Exception: If manifest file is missing or malformed.
        """
        manifest_path = f"{self.input_prefix}/manifest.json"
        try:
            logger.info(f"Reading manifest from {manifest_path}")
            manifest_df = self.spark.read.json(manifest_path)
            keys = manifest_df.first()["uploaded_keys"]
            return [f"s3a://{self.bucket}/{key}" for key in keys]
        except Exception as e:
            logger.error(f"Failed to read manifest: {e}")
            raise

    def _read_raw_data(self, file_paths: List[str]):
        """
        Reads the raw transaction data from a list of JSON files.

        Args:
            file_paths: List of full S3 URIs to JSON files.

        Returns:
            Spark DataFrame with raw data.
        """
        try:
            return self.spark.read.json(file_paths)
        except Exception as e:
            logger.error(f"Failed to read JSON files: {e}")
            raise

    def _write_raw_data(self, df):
        """
        Writes raw transaction data to the curated output in Parquet format.

        Args:
            df: Spark DataFrame to write.

        Raises:
            Exception: On write failure.
        """
        try:
            df.write.mode("overwrite").partitionBy("payment_date").parquet(self.output_raw_path)
            logger.info(f"Raw data written to {self.output_raw_path}")
        except Exception as e:
            logger.error(f"Failed to write raw data: {e}")
            raise

    def _write_aggregated_data(self, df):
        """
        Writes aggregated metrics to curated output in Parquet format.

        Args:
            df: Spark DataFrame with aggregated metrics.

        Raises:
            Exception: On write failure.
        """
        try:
            df.write.mode("overwrite").partitionBy("payment_date").parquet(self.output_agg_path)
            logger.info(f"Aggregated data written to {self.output_agg_path}")
        except Exception as e:
            logger.error(f"Failed to write aggregated data: {e}")
            raise

    def run(self):
        """
        Executes the full ETL pipeline:
        - Reads manifest
        - Loads raw transaction JSONs
        - Writes raw and aggregated Parquet tables

        Raises:
            Exception: If any step of the ETL fails.
        """
        try:
            self._init_spark()

            file_paths = self._load_file_paths_from_manifest()
            df = self._read_raw_data(file_paths)

            # Save raw data
            self._write_raw_data(df)

            # Filter successful and compute aggregations
            df_success = df.filter(col("status") == "success")
            df_agg = df_success.groupBy("game", "payment_date").agg(
                _sum("price").alias("total_revenue"),
                count("*").alias("successful_transactions")
            )

            # Save aggregates
            self._write_aggregated_data(df_agg)

            logger.info("ETL job completed successfully.")

        except Exception as e:
            logger.error(f"ETL failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
