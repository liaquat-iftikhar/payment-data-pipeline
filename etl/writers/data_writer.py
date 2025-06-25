import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DataWriter:
    def __init__(self, output_path: str):
        self.output_path = output_path

    def write_partitioned(self, df: DataFrame, partition_col: str = "payment_date"):
        try:
            df.write.mode("overwrite").partitionBy(partition_col).parquet(self.output_path)
            logger.info(f"Data written to {self.output_path}")
        except Exception as e:
            logger.error(f"Failed to write data to {self.output_path}: {e}")
            raise