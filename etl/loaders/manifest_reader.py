import logging
from typing import List
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ManifestReader:
    def __init__(self, spark: SparkSession, manifest_path: str, bucket: str):
        self.spark = spark
        self.manifest_path = manifest_path
        self.bucket = bucket

    def get_file_paths(self) -> List[str]:
        try:
            logger.info(f"Reading manifest from {self.manifest_path}")
            manifest_df = self.spark.read.json(self.manifest_path)
            keys = manifest_df.first()["uploaded_keys"]
            return [f"s3a://{self.bucket}/{key}" for key in keys]
        except Exception as e:
            logger.error(f"Failed to read manifest: {e}")
            raise