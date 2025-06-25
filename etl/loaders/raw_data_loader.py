import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class RawDataLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load(self, file_paths: List[str]) -> DataFrame:
        try:
            return self.spark.read.json(file_paths)
        except Exception as e:
            logger.error(f"Failed to read JSON files: {e}")
            raise