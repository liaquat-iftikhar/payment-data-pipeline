import pytest
from unittest.mock import MagicMock
from pipeline.fetcher.payment_data_fetcher import PaymentDataFetcher

class TestPaymentDataFetcherUnit:

    def test_run_no_data(self):
        mock_api_client = MagicMock()
        mock_api_client.fetch_page.return_value = {"data": [], "has_more": False}

        fetcher = PaymentDataFetcher(
            api_client=mock_api_client,
            uploader=MagicMock(),
            manifest_writer=MagicMock()
        )

        result = fetcher.run("2025-01-01", "2025-01-02")
        assert result["status"] == "no_data"
        assert result["num_files"] == 0
