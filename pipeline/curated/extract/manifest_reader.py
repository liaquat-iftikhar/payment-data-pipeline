import logging
from typing import List
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ManifestReader:
    """
    Reads a manifest.json file from S3 using Spark and extracts the list of uploaded file paths.

    Attributes:
        spark (SparkSession): Spark session used for reading the manifest.
        manifest_file_info (dict): Dictionary containing manifest metadata, including path.
    """

    def __init__(self, spark: SparkSession, manifest_file_info: dict):
        self.spark = spark
        self.manifest_file_info = manifest_file_info
        self.manifest_path = manifest_file_info.get("manifest_path")

        if not self.manifest_path:
            raise ValueError("Missing required manifest metadata: 'bucket_name' or 'manifest_path'.")

    def get_file_paths(self) -> List[str]:
        """
        Extracts full S3 paths from the manifest file.

        Returns:
            List[str]: List of full S3 URIs to the uploaded data files.

        Raises:
            Exception: If reading or parsing the manifest fails.
        """
        try:
            logger.info(f"Reading manifest file from: {self.manifest_path}")
            manifest_df = self.spark.read.json(self.manifest_path)
            record = manifest_df.first()

            if not record or "uploaded_keys" not in record:
                raise ValueError("Manifest does not contain 'uploaded_keys'.")

            keys = record["uploaded_keys"]
            if not isinstance(keys, list) or not keys:
                raise ValueError("Manifest 'uploaded_keys' is empty or malformed.")

            logger.info(f"Found {len(keys)} files in manifest.")
            return keys

        except Exception as e:
            logger.error(f"Failed to read or parse manifest file: {e}")
            raise RuntimeError("Manifest reading failed.") from e
