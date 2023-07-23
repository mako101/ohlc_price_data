import unittest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from csv_processor.api import app


class TestCSVProcessor(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.mock_db_session = MagicMock()
        self.mock_spark_session = MagicMock()

    def test_upload_csv_invalid_file_type(self):
        # Test uploading a non-CSV file
        with patch("tempfile.NamedTemporaryFile", return_value=MagicMock()) as mock_tmp_file:
            response = self.client.post("/data/", files={"file": ("non_csv_file.txt", b"some content")})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"detail": "Only CSV files are allowed."})

    def test_upload_csv_invalid_schema(self):
        # Test uploading a CSV file with an invalid schema

        csv_data = """
        timestamp,symbol,open_price,highest,lowest
        2022-02-13 02:31:00,BTCUSDT,30000,31000,29000
        """
        with patch("tempfile.NamedTemporaryFile", return_value=MagicMock()) as mock_tmp_file:
            response = self.client.post("/data/", files={"file": ("test.csv", csv_data.encode())})

        self.assertEqual(response.status_code, 422)
        self.assertIn("Data Validation Failed", response.json()["detail"])
