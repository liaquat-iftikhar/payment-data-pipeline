import requests
import time
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PaymentAPIClient:
    """
    A client for interacting with a paginated payment API.

    Features:
    - Exponential backoff retry strategy
    - Configurable timeout and retry logic

    Args:
        base_url (str): Base URL of the payment API.
        max_retries (int): Maximum number of retry attempts on request failure.
        backoff_factor (float): Backoff multiplier for exponential wait between retries.
    """

    def __init__(self, base_url: str, max_retries: int = 3, backoff_factor: float = 2.0):
        self.base_url = base_url.rstrip("?&")
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def fetch_page(self, start_date: str, end_date: str, page: int) -> Dict[str, Any]:
        """
        Fetches a page of payment data from the API.

        Args:
            start_date (str): Start date in YYYY-MM-DD format.
            end_date (str): End date in YYYY-MM-DD format.
            page (int): Page number to fetch.

        Returns:
            dict: Parsed JSON response from the API.

        Raises:
            RuntimeError: If all retry attempts fail.
        """
        url = f"{self.base_url}?start_date={start_date}&end_date={end_date}&page={page}"

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Fetching page {page} from API: {url}")
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                logger.info(f"Page {page} fetched successfully.")
                return response.json()

            except requests.RequestException as e:
                logger.warning(f"API request failed on attempt {attempt}/{self.max_retries}: {e}")
                if attempt == self.max_retries:
                    logger.error(f"Max retries exceeded for page {page}.")
                    raise RuntimeError(f"Max retries exceeded for page {page}.") from e

                sleep_time = self.backoff_factor ** (attempt - 1)
                logger.info(f"Retrying after {sleep_time:.1f}s...")
                time.sleep(sleep_time)
