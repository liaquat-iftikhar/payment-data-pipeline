import logging
from typing import Dict, Any

from pipeline.fetcher.manifest_writer import ManifestWriter
from pipeline.fetcher.payment_api_client import PaymentAPIClient
from pipeline.fetcher.s3_uploader import S3Uploader
from pipeline.fetcher.payment_data_fetcher import PaymentDataFetcher

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PaymentDataIngestionService:
    """
    Orchestrates the end-to-end process of fetching payment data from an external API,
    uploading the data to S3, and creating a manifest file.

    This service wraps the setup and execution of the fetcher pipeline, providing a
    single entry point for ingestion.

    Attributes:
        s3_bucket (str): Target S3 bucket.
        api_base_url (str): Base URL of the payment API.
        max_retries (int): Max retry attempts for API calls.
        backoff_factor (float): Exponential backoff factor.
    """

    def __init__(
        self,
        s3_bucket: str,
        api_base_url: str,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        self.s3_bucket = s3_bucket
        self.api_base_url = api_base_url
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def run(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Executes the ingestion process for a given date range.

        Args:
            start_date (str): Start date (YYYY-MM-DD).
            end_date (str): End date (YYYY-MM-DD).

        Returns:
            Dict[str, Any]: Summary of the ingestion result, including manifest location and status.
        """
        logger.info(f"Starting payment data ingestion for {start_date} to {end_date}")

        # Step 1: Create core components
        api_client = PaymentAPIClient(
            base_url=self.api_base_url,
            max_retries=self.max_retries,
            backoff_factor=self.backoff_factor
        )

        uploader = S3Uploader(bucket_name=self.s3_bucket)
        manifest_writer = ManifestWriter(uploader=uploader)

        # Step 2: Run data fetcher
        fetcher = PaymentDataFetcher(
            api_client=api_client,
            uploader=uploader,
            manifest_writer=manifest_writer
        )

        result = fetcher.run(start_date=start_date, end_date=end_date)

        logger.info(f"Ingestion result: {result}")
        return result
