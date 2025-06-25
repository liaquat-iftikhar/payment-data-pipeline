"""
Module: payment_data_fetcher

This module defines a PaymentDataFetcher class that retrieves paginated payment data
from a REST API and uploads it to an S3 bucket. It also writes a manifest file listing
all uploaded pages and a _SUCCESS marker file to indicate that ingestion completed successfully.

The fetcher supports retry logic with exponential backoff and is configurable via
an injected settings object (e.g., from Dynaconf).
"""

import requests
import json
import boto3
import time
import logging
from typing import Any
from botocore.exceptions import BotoCoreError, ClientError
from dynaconf import Dynaconf

# Setup basic logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PaymentDataFetcher:
    """
    Fetches paginated payment data from an external API and uploads each page to AWS S3.
    Includes manifest and _SUCCESS marker for downstream processing coordination.
    """

    def __init__(self, settings: Dynaconf):
        """
        Initialize the fetcher with required configuration.

        Args:
            settings: A settings object (e.g., from Dynaconf) that includes:
                - bucket_name: Name of the target S3 bucket.
                - payment_api_base_url: Base URL of the payment report API.
                - max_retries: Max number of retry attempts on API failure.
                - backoff_factor: Exponential backoff multiplier.
        """
        self.settings = settings
        self.bucket_name = settings.bucket_name
        self.api_base_url = settings.payment_api_base_url.rstrip("?&")
        self.max_retries = settings.max_retries
        self.backoff_factor = settings.backoff_factor
        self.s3 = boto3.client("s3")

    def _get_page(self, start_date: str, end_date: str, page: int) -> dict:
        """
        Fetch a single page of payment data from the API.

        Args:
            start_date (str): Start date in YYYY-MM-DD format.
            end_date (str): End date in YYYY-MM-DD format.
            page (int): Page number to fetch.

        Returns:
            dict: Parsed JSON response containing a page of payment records.

        Raises:
            RuntimeError: If max retries are exceeded.
        """
        url = f"{self.api_base_url}?start_date={start_date}&end_date={end_date}&page={page}"

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Fetching page {page} from API: {url}")
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                logger.warning(f"API request failed on attempt {attempt}: {e}")
                if attempt == self.max_retries:
                    raise RuntimeError(f"Max retries exceeded for page {page}.") from e
                time.sleep(self.backoff_factor ** (attempt - 1))

    def _upload_page_to_s3(self, data: list[dict[str, Any]], key: str) -> None:
        """
        Upload a single page of JSON payment data to S3.

        Args:
            data (list): List of transaction dictionaries.
            key (str): S3 object key where the data will be stored.

        Raises:
            RuntimeError: If the upload fails.
        """
        try:
            logger.info(f"Uploading page to s3://{self.bucket_name}/{key}")
            self.s3.put_object(
                Body=json.dumps(data),
                Bucket=self.bucket_name,
                Key=key,
                ContentType="application/json"
            )
        except (BotoCoreError, ClientError, ValueError) as e:
            logger.error(f"Failed to upload {key}: {e}")
            raise RuntimeError(f"S3 upload failed for {key}") from e

    def _upload_manifest(self, prefix: str, keys: list[str]) -> None:
        """
        Upload a manifest.json file listing all successfully uploaded page keys.

        Args:
            prefix (str): S3 prefix (folder path).
            keys (list): List of uploaded object keys.

        Raises:
            RuntimeError: If manifest upload fails.
        """
        key = f"{prefix}/manifest.json"
        try:
            logger.info(f"Uploading manifest to s3://{self.bucket_name}/{key}")
            self.s3.put_object(
                Body=json.dumps({"uploaded_keys": keys}, indent=2),
                Bucket=self.bucket_name,
                Key=key,
                ContentType="application/json"
            )
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError("Failed to write manifest.json") from e

    def run(self, start_date: str, end_date: str) -> None:
        """
        Main entry point to fetch and upload paginated payment data.

        - Pages are fetched one-by-one from the API.
        - Each page is uploaded to S3.
        - A manifest is written listing all uploaded pages.
        - A _SUCCESS marker is added if all uploads succeed.

        Args:
            start_date (str): Start date in YYYY-MM-DD format.
            end_date (str): End date in YYYY-MM-DD format.
        """
        page = 1
        has_more = True
        s3_prefix = f"raw/payments_{start_date}_{end_date}"
        uploaded_keys = []

        while has_more:
            try:
                result = self._get_page(start_date, end_date, page)
                data = result.get("data", [])
                has_more = result.get("has_more", False)

                if not data:
                    logger.info(f"No data returned for page {page}")
                    break

                key = f"{s3_prefix}/page_{page}.json"
                self._upload_page_to_s3(data, key)
                uploaded_keys.append(key)
                logger.info(f"Page {page} processed successfully.")
                page += 1

            except Exception as e:
                logger.error(f"Error processing page {page}: {e}")
                break

        if uploaded_keys:
            self._upload_manifest(s3_prefix, uploaded_keys)
            logger.info("ETL complete. Manifest and success marker written.")
        else:
            logger.warning("No data was uploaded. Success marker and manifest not created.")
