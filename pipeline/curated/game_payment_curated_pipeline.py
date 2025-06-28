import logging
from typing import List

from pipeline.curated.extract.manifest_reader import ManifestReader
from pipeline.curated.extract.raw_data_extractor import RawDataExtractor
from pipeline.utils.spark_session_builder import SparkSessionBuilder
from pipeline.utils.s3_partitioned_data_loader import S3PartitionedDataLoader

import pyspark.sql.functions as F

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class GamePaymentCuratedPipeline:
    """
    Pipeline to load raw game payment data into a curated S3 Parquet dataset and
    register it as a partitioned Athena table via Glue catalog.

    Steps:
    - Read manifest to discover raw files
    - Read raw JSON into Spark DataFrame
    - Write data to partitioned Parquet in S3
    - Register external table with Glue (Athena compatible)

    Attributes:
        manifest_file_info (dict): Contains bucket name and manifest path
        output_path (str): Target S3 output path for curated data
        database (str): Glue catalog database name
        table (str): Glue catalog table name
        partition_col (str): Column used for partitioning (e.g. payment_date)
    """

    REQUIRED_COLUMNS = {"transaction_id", "game", "price", "currency", "status"}

    def __init__(
        self,
        manifest_file_info: dict,
        output_path: str,
        database: str,
        table: str,
        partition_col: str,
    ):
        self.manifest_file_info = manifest_file_info
        self.curated_output = output_path.rstrip("/")
        self.database = database
        self.table = table
        self.partition_col = partition_col

    def _get_ddl_queries(self) -> List[str]:
        """
        Generates DDL SQL statements to create and repair the Athena table.

        Returns:
            List[str]: List of Spark SQL DDL queries
        """
        return [
            f"CREATE DATABASE IF NOT EXISTS {self.database}",
            f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{self.table} (
                    transaction_id STRING,
                    game STRING,
                    price DOUBLE,
                    currency STRING,
                    status STRING
                )
                PARTITIONED BY ({self.partition_col} STRING)
                STORED AS PARQUET
                LOCATION 's3://{self.curated_output}'
            """,
            f"MSCK REPAIR TABLE {self.database}.{self.table}"
        ]

    def _validate_data(self, df) -> None:
        logger.info("Running data quality checks on raw DataFrame...")

        # Check if empty
        if df.rdd.isEmpty():
            logger.error("Input DataFrame is empty.")
            raise ValueError("Empty DataFrame: no data to process.")

        # Check required columns
        missing = self.REQUIRED_COLUMNS - set(df.columns)
        if missing:
            logger.error(f"Missing required columns: {missing}")
            raise ValueError(f"Missing columns: {missing}")

        # Check for nulls in critical fields
        for col in ["transaction_id", "currency", "status"]:
            nulls = df.filter(F.col(col).isNull()).count()
            if nulls > 0:
                logger.error(f"Column '{col}' has {nulls} null values.")
                raise ValueError(f"Column '{col}' contains nulls.")

        # Check for negative price
        negative_prices = df.filter(F.col("price") < 0).count()
        if negative_prices > 0:
            logger.error(f"'price' has {negative_prices} negative values.")
            raise ValueError("Invalid values in 'price': cannot be negative.")

        logger.info("Data quality checks passed.")

    def run(self) -> List[str]:
        """
        Executes the full ETL pipeline.

        Returns:
            List[str]: List of full S3 paths of successfully loaded partitions.

        Raises:
            Exception: If any step of the pipeline fails.
        """
        with SparkSessionBuilder(app_name="GamePaymentCuratedPipeline") as spark:
            try:
                logger.info("Starting curated data pipeline...")

                manifest_reader = ManifestReader(spark, self.manifest_file_info)
                data_extractor = RawDataExtractor(spark)

                raw_files = manifest_reader.get_file_paths()
                logger.info(f"Found {len(raw_files)} raw files to process.")

                raw_df = data_extractor.extract(raw_files)
                logger.info("Raw data successfully loaded into Spark DataFrame.")

                # Data quality check
                self._validate_data(raw_df)
                logger.info("Data Quality checks executed successfully.")

                # Write to S3 and collect updated partitions
                partitions = S3PartitionedDataLoader(
                    raw_df,
                    self.curated_output,
                    self.partition_col
                ).load()

                loaded_partitions = [
                    f"s3://{self.curated_output}/{self.partition_col}={partition}"
                    for partition in partitions
                ]
                logger.info(f"Data written to {len(loaded_partitions)} partitions.")

                # Register with Glue/Athena
                for query in self._get_ddl_queries():
                    spark.sql(query)
                logger.info("Athena table creation/repair completed.")

                logger.info("Curated pipeline completed successfully.")
                return loaded_partitions

            except Exception as e:
                logger.exception("GamePaymentCuratedPipeline failed.")
                raise RuntimeError("Pipeline execution failed.") from e
