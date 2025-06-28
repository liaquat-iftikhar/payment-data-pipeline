import pytest
from unittest.mock import patch, MagicMock
from pipeline.fetcher.payment_data_ingestion_service import PaymentDataIngestionService

@pytest.fixture
def mock_api_response():
    return {
        "data": [
            {"transaction_id": "tx1", "price": 15, "currency": "USD", "status": "SUCCESS"},
            {"transaction_id": "tx2", "price": 20, "currency": "EUR", "status": "SUCCESS"},
        ],
        "has_more": False,
    }

@patch("pipeline.fetcher.payment_api_client.requests.get")
@patch("pipeline.fetcher.s3_uploader.boto3.client")
def test_ingestion_service_run(mock_boto_client, mock_requests_get, mock_api_response):
    # Mock API response
    mock_requests_get.return_value.status_code = 200
    mock_requests_get.return_value.json.return_value = mock_api_response

    # Mock S3 client put_object (simulate success)
    mock_s3_client = MagicMock()
    mock_boto_client.return_value = mock_s3_client
    mock_s3_client.put_object.return_value = {}

    ingestion_service = PaymentDataIngestionService(
        s3_bucket="test-bucket",
        api_base_url="https://fake-api/payments"
    )

    result = ingestion_service.run(start_date="2025-01-01", end_date="2025-01-01")

    assert result["status"] == "success"
    assert result["num_files"] == 1
    assert result["manifest_path"].startswith("s3://test-bucket")

    mock_requests_get.assert_called()
    mock_s3_client.put_object.assert_called()

