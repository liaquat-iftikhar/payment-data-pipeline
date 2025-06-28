import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class RawDataExtractor:
    """
    Extracts raw payment data from a list of JSON files into a Spark DataFrame.

    Attributes:
        spark (SparkSession): A SparkSession instance for reading data.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def extract(self, file_paths: List[str]) -> DataFrame:
        """
        Reads a list of JSON files into a Spark DataFrame.

        Args:
            file_paths (List[str]): Full S3 paths (or local paths) to JSON files.

        Returns:
            DataFrame: A Spark DataFrame containing the parsed data.

        Raises:
            ValueError: If file_paths is empty or None.
            RuntimeError: If reading the files fails.
        """
        if not file_paths:
            logger.warning("No file paths provided for extraction.")
            raise ValueError("No input files provided to extract method.")

        try:
            logger.info(f"Reading {len(file_paths)} JSON files.")
            df = self.spark.read.json(file_paths)
            logger.info("Raw data extraction successful.")
            return df

        except Exception as e:
            logger.error(f"Failed to read JSON files: {e}")
            raise RuntimeError("Error while reading raw JSON files.") from e
