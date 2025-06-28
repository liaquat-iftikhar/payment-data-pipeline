import logging
from pyspark.sql import SparkSession

from pipeline.enriched.extract.curated_data_extractor import CuratedDataExtractor
from pipeline.enriched.transform.successful_game_payment_aggregator import SuccessfulGamePaymentAggregator
from pipeline.utils.s3_partitioned_data_loader import S3PartitionedDataLoader
from pipeline.utils.spark_session_builder import SparkSessionBuilder

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class EnrichedGamePaymentPipeline:
    """
    Pipeline to process enriched aggregated game payment data from curated input.

    Responsibilities:
    - Reading partitioned curated data from S3
    - Aggregating successful payment transactions per game per day
    - Writing aggregated data back to S3 partitioned by date
    - Registering the output as an external Athena table
    """

    def __init__(
            self,
            input_partitions: list,
            enriched_output: str,
            database: str,
            table: str,
            partition_col: str,
    ):
        self.input_partitions = input_partitions
        self.enriched_output = enriched_output.rstrip('/')
        self.database = database
        self.table = table
        self.partition_col = partition_col

    def _register_table(self, spark: SparkSession) -> None:
        """Creates the Athena external table if it does not exist and repairs partitions."""
        ddl_statements = [
            f"CREATE DATABASE IF NOT EXISTS {self.database}",
            f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {self.database}.{self.table} (
                game STRING,
                total_revenue DOUBLE,
                successful_transactions BIGINT
            )
            PARTITIONED BY ({self.partition_col} STRING)
            STORED AS PARQUET
            LOCATION '{self.enriched_output}'
            """,
            f"MSCK REPAIR TABLE {self.database}.{self.table}"
        ]

        for stmt in ddl_statements:
            logger.info(f"Executing SQL: {stmt.strip().splitlines()[0]} ...")
            spark.sql(stmt)

    def run(self) -> list:
        """
        Runs the enriched data ETL pipeline.

        Returns:
            List[str]: List of S3 paths for the updated partitions.

        Raises:
            Exception: Propagates any exception that occurs during ETL.
        """
        with SparkSessionBuilder(app_name="EnrichedGamePaymentPipeline") as spark:
            try:
                logger.info("Extracting curated data from partitions.")
                curated_df = CuratedDataExtractor(spark).extract(self.input_partitions)

                logger.info("Aggregating successful game payments.")
                aggregated_df = SuccessfulGamePaymentAggregator(curated_df).transform()

                logger.info("Writing aggregated data partitioned by %s.", self.partition_col)
                partitions = S3PartitionedDataLoader(aggregated_df, self.enriched_output, self.partition_col).load()

                loaded_partitions = [
                    f"{self.enriched_output}/{self.partition_col}={partition}"
                    for partition in partitions
                ]

                logger.info("Registering Athena table and repairing partitions.")
                self._register_table(spark)

                logger.info(f"Enriched ETL pipeline completed successfully. Updated partitions: {loaded_partitions}")
                return loaded_partitions

            except Exception as ex:
                logger.error(f"Enriched ETL pipeline failed: {ex}", exc_info=True)
                raise
