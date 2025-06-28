import logging
from typing import List

from pipeline.fetcher.s3_uploader import S3Uploader

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ManifestWriter:
    """
    Handles creation and upload of a manifest file to S3.

    Attributes:
        uploader (S3Uploader): The uploader instance responsible for S3 operations.
    """

    def __init__(self, uploader: S3Uploader):
        """
        Initializes the ManifestWriter with a given S3Uploader.

        Args:
            uploader (S3Uploader): Uploader to handle JSON upload to S3.
        """
        self.uploader = uploader

    def write_manifest(self, prefix: str, full_paths: List[str]) -> str:
        """
        Writes a manifest file to S3 containing the list of uploaded file paths.

        Args:
            prefix (str): The S3 prefix (directory path) under which the manifest will be saved.
            full_paths (List[str]): A list of fully qualified S3 paths to include in the manifest.

        Returns:
            str: The full S3 key of the uploaded manifest file.

        Raises:
            RuntimeError: If the upload fails.
        """
        key = f"{prefix.rstrip('/')}/manifest.json"
        manifest_data = {"uploaded_keys": full_paths}

        try:
            logger.info(f"Uploading manifest to s3://{self.uploader.bucket_name}/{key}")
            return self.uploader.upload_json(manifest_data, key)
        except Exception as e:
            logger.error(f"Failed to upload manifest to S3: {e}")
            raise RuntimeError("Failed to write manifest.json") from e
