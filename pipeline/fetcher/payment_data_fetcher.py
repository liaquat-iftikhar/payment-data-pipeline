
import logging
from typing import List, Dict, Any

from pipeline.fetcher.manifest_writer import ManifestWriter
from pipeline.fetcher.payment_api_client import PaymentAPIClient
from pipeline.fetcher.s3_uploader import S3Uploader

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PaymentDataFetcher:
    """Coordinates the full fetch-upload-manifest-write flow."""

    def __init__(self, api_client: PaymentAPIClient, uploader: S3Uploader, manifest_writer: ManifestWriter):
        self.api_client = api_client
        self.uploader = uploader
        self.manifest_writer = manifest_writer

    def _validate_data(self, data: List[Dict[str, Any]]) -> bool:
        """
        Basic data quality checks on the payment data.

        Checks:
          - transaction_id is not null or empty
          - price is non-negative
          - currency is in allowed values
          - status is in allowed values

        Returns:
            bool: True if all records pass validation, False otherwise.
        """
        allowed_currencies = {"USD", "EUR"}
        allowed_statuses = {"SUCCESS", "FAILED"}

        for idx, record in enumerate(data):
            if not record.get("transaction_id"):
                logger.error(f"Record {idx} missing transaction_id: {record}")
                return False
            if record.get("price", -1) < 0:
                logger.error(f"Record {idx} has negative price: {record}")
                return False
            if record.get("currency") not in allowed_currencies:
                logger.error(f"Record {idx} has invalid currency: {record}")
                return False
            if record.get("status") not in allowed_statuses:
                logger.error(f"Record {idx} has invalid status: {record}")
                return False
        return True

    def run(self, start_date: str, end_date: str) -> Dict[str, Any]:
        prefix = f"raw/payments_{start_date}_{end_date}"
        uploaded_paths: List[str] = []

        page = 1
        has_more = True

        while has_more:
            try:
                result = self.api_client.fetch_page(start_date, end_date, page)
                data = result.get("data", [])
                has_more = result.get("has_more", False)

                if not data:
                    logger.info(f"No data on page {page}")
                    break

                # Validate data quality before upload
                if not self._validate_data(data):
                    logger.error(f"Data quality validation failed for page {page}. Aborting ingestion.")
                    break

                key = f"{prefix}/page_{page}.json"
                full_path = self.uploader.upload_json(data, key)
                uploaded_paths.append(full_path)

                logger.info(f"Page {page} uploaded successfully.")
                page += 1

            except Exception as e:
                logger.error(f"Error processing page {page}: {e}")
                break

        if uploaded_paths:
            manifest_s3_uri = self.manifest_writer.write_manifest(prefix, uploaded_paths)
            self.uploader.upload_marker(prefix)
            logger.info("Manifest and _SUCCESS marker written.")
            return {
                "status": "success",
                "manifest_path": manifest_s3_uri,
                "num_files": len(uploaded_paths)
            }

        else:
            logger.warning("No data found. No files uploaded.")
            return {
                "status": "no_data",
                "manifest_path": None,
                "num_files": 0
            }