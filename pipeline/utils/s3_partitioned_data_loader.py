import logging
from typing import List
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class S3PartitionedDataLoader:
    """
    A generic utility class to write a Spark DataFrame to S3 in partitioned Parquet format.

    Attributes:
        df (DataFrame): The Spark DataFrame to write.
        output_path (str): Target S3 location (e.g., 's3a://bucket/path/').
        partition_col (str): Column to partition data by.
    """

    def __init__(self, df: DataFrame, output_path: str, partition_col: str):
        """
        Initializes the loader.

        Args:
            df (DataFrame): The Spark DataFrame to write.
            output_path (str): Destination S3 path.
            partition_col (str): Column name to partition by.
        """
        self.df = df
        self.output_path = output_path.rstrip('/')
        self.partition_col = partition_col

    def _write_to_s3(self) -> None:
        """Writes the DataFrame to S3 in Parquet format partitioned by the specified column."""
        try:
            self.df.write.mode("overwrite").partitionBy(self.partition_col).parquet(self.output_path)
            logger.info(f"Data successfully written to {self.output_path}")
        except Exception as e:
            logger.error(f"Failed to write data to {self.output_path}", exc_info=True)
            raise

    def _extract_partitions(self) -> List[str]:
        """
        Extracts the distinct partition values from the DataFrame.

        Returns:
            List[str]: List of unique values from the partition column.
        """
        try:
            partitions = (
                self.df.select(self.partition_col)
                .distinct()
                .rdd.flatMap(lambda row: row)
                .collect()
            )
            logger.info(f"Extracted partitions: {partitions}")
            return partitions
        except Exception as e:
            logger.warning(f"Could not extract partition values from DataFrame: {e}", exc_info=True)
            return []

    def load(self) -> List[str]:
        """
        Executes the full data load process: writes DataFrame and returns list of partitions.

        Returns:
            List[str]: A list of distinct partition values that were written to S3.
        """
        self._write_to_s3()
        return self._extract_partitions()
