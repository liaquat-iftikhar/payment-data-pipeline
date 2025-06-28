import json
import boto3
import logging
from typing import Any, Optional
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class S3Uploader:
    """
    Utility class for uploading data to Amazon S3.

    Attributes:
        bucket (str): Target S3 bucket name.
        s3 (boto3.client): Boto3 S3 client.
    """

    def __init__(self, bucket_name: str, s3_client: Optional[Any] = None):
        """
        Initializes the uploader with a bucket and an optional S3 client.

        Args:
            bucket_name (str): Name of the S3 bucket.
            s3_client (boto3.client, optional): Custom boto3 client, useful for testing.
        """
        self.bucket = bucket_name
        self.s3 = s3_client or boto3.client("s3")

    def upload_json(self, data: Any, key: str) -> str:
        """
        Uploads a JSON-serializable object to S3.

        Args:
            data (Any): JSON-serializable content.
            key (str): S3 object key (path within the bucket).

        Returns:
            str: Full S3 URI of the uploaded file.

        Raises:
            RuntimeError: If the upload fails.
        """
        try:
            logger.info(f"Uploading JSON to s3://{self.bucket}/{key}")
            self.s3.put_object(
                Body=json.dumps(data, indent=2),
                Bucket=self.bucket,
                Key=key,
                ContentType="application/json"
            )
            return f"s3://{self.bucket}/{key}"

        except (BotoCoreError, ClientError, ValueError) as e:
            logger.error(f"Upload failed for s3://{self.bucket}/{key}: {e}")
            raise RuntimeError(f"S3 upload failed for {key}") from e

    def upload_marker(self, prefix: str, marker_name: str = "_SUCCESS") -> str:
        """
        Uploads a marker file to S3, typically used to signal successful completion.

        Args:
            prefix (str): S3 prefix where marker will be stored.
            marker_name (str): Marker file name (default: "_SUCCESS").

        Returns:
            str: Full S3 URI of the uploaded marker.

        Raises:
            RuntimeError: If the marker upload fails.
        """
        key = f"{prefix.rstrip('/')}/{marker_name}"
        try:
            logger.info(f"Uploading marker to s3://{self.bucket}/{key}")
            self.s3.put_object(Body="", Bucket=self.bucket, Key=key)
            return f"s3://{self.bucket}/{key}"

        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to upload marker file {marker_name} to s3://{self.bucket}/{key}: {e}")
            raise RuntimeError(f"Failed to upload marker file {marker_name}") from e
