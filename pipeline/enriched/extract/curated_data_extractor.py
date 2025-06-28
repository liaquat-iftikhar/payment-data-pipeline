import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class CuratedDataExtractor:
    """
    Extracts curated Parquet data from a list of S3 partition paths into a Spark DataFrame.

    Attributes:
        spark (SparkSession): An active Spark session.
    """

    def __init__(self, spark: SparkSession):
        """
        Initializes the extractor with a Spark session.

        Args:
            spark (SparkSession): The Spark session used for reading data.
        """
        self.spark = spark

    def extract(self, upserted_partitions: List[str]) -> DataFrame:
        """
        Loads data from a list of partition paths (S3 URIs or HDFS/local paths).

        Args:
            upserted_partitions (List[str]): List of S3 paths to Parquet partitions.

        Returns:
            DataFrame: Spark DataFrame containing the combined data.

        Raises:
            ValueError: If the list is empty.
            RuntimeError: If reading the data fails.
        """
        if not upserted_partitions:
            logger.warning("No partition paths provided for extraction.")
            raise ValueError("Cannot extract: partition list is empty.")

        try:
            logger.info(f"Reading {len(upserted_partitions)} partition(s)...")
            df = self.spark.read.parquet(*upserted_partitions)
            logger.info("Curated data successfully read.")
            return df

        except Exception as e:
            logger.error(f"Failed to read Parquet files: {e}")
            raise RuntimeError("Error reading curated Parquet partitions.") from e
