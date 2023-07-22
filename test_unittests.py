import unittest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from datetime import datetime
from csv_processor.api import app, spark
from csv_processor.models import Position
import csv_processor.utils as u


class TestCSVProcessor(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.mock_session = MagicMock()

    @patch("csv_processor.api.get_db_session")
    def test_upload_csv_invalid_file_type(self, mock_get_db_session):
        # Test uploading a non-CSV file
        mock_get_db_session.return_value = self.mock_session

        with patch("tempfile.NamedTemporaryFile", return_value=MagicMock()) as mock_tmp_file:
            response = self.client.post("/data/", files={"file": ("non_csv_file.txt", b"some content")})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"detail": "Only CSV files are allowed."})
        mock_tmp_file.assert_not_called()
        self.mock_session.assert_not_called()

    @patch("csv_processor.api.spark.read.csv")
    @patch("csv_processor.api.get_db_session")
    def test_upload_csv_valid_file(self, mock_get_db_session, mock_spark_read_csv):
        # Test uploading a valid CSV file
        mock_get_db_session.return_value = self.mock_session

        csv_data = """timestamp,symbol,open_price,highest,lowest,close_price
        2022-02-13 02:31:00,BTCUSDT,30000,31000,29000,30500
        2022-02-13 02:32:00,ETHUSDT,2000,2100,1950,2050
        """
        expected_df_data = [
            ("2022-02-13 02:31:00", "BTCUSDT", 30000, 31000, 29000, 30500),
            ("2022-02-13 02:32:00", "ETHUSDT", 2000, 2100, 1950, 2050),
        ]

        mock_tmp_file = MagicMock()
        mock_tmp_file.__enter__.return_value.name = "temp.csv"
        with patch("tempfile.NamedTemporaryFile", return_value=mock_tmp_file):
            response = self.client.post("/data/", files={"file": ("test.csv", csv_data.encode())})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'result': 'success', 'detail': 'Processed 2 entries from test.csv'})

        self.mock_session.assert_called()
        self.mock_session.commit.assert_called()

        # Verify the DataFrame data passed to PySpark
        mock_session = spark.read.csv("temp.csv", header=True, inferSchema=True)
        actual_df_data = [(row.timestamp, row.symbol, row.open_price, row.highest, row.lowest, row.close_price)
                          for row in mock_session.collect()]

        self.assertEqual(actual_df_data, expected_df_data)

    @patch("csv_processor.api.get_db_session")
    def test_upload_csv_invalid_schema(self, mock_get_db_session):
        # Test uploading a CSV file with an invalid schema
        mock_get_db_session.return_value = self.mock_session

        csv_data = """timestamp,symbol,open_price,highest,lowest
        2022-02-13 02:31:00,BTCUSDT,30000,31000,29000  # Missing close_price
        """
        with patch("tempfile.NamedTemporaryFile", return_value=MagicMock()) as mock_tmp_file:
            response = self.client.post("/data/", files={"file": ("test.csv", csv_data.encode())})

        self.assertEqual(response.status_code, 422)
        self.assertIn("Data Validation Failed", response.json()["detail"])
        mock_tmp_file.assert_not_called()
        self.mock_session.assert_not_called()

    @patch("csv_processor.api.get_db_session")
    def test_get_records(self, mock_get_db_session):
        # Test getting stock records
        mock_get_db_session.return_value = self.mock_session

        mock_query = MagicMock()
        self.mock_session.query.return_value = mock_query
        mock_query.count.return_value = 2

        mock_record_1 = Position(timestamp=datetime(2022, 2, 13, 2, 31), symbol="BTCUSDT", open_price=30000,
                                  highest=31000, lowest=29000, close_price=30500)
        mock_record_2 = Position(timestamp=datetime(2022, 2, 13, 2, 32), symbol="ETHUSDT", open_price=2000,
                                  highest=2100, lowest=1950, close_price=2050)
        mock_records = [mock_record_1, mock_record_2]
        mock_query.limit.return_value.offset.return_value.all.return_value = mock_records

        response = self.client.get("/data")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"], [
            {'timestamp': '2022-02-13T02:31:00', 'symbol': 'BTCUSDT', 'open_price': 30000,
             'highest': 31000, 'lowest': 29000, 'close_price': 30500},
            {'timestamp': '2022-02-13T02:32:00', 'symbol': 'ETHUSDT', 'open_price': 2000,
             'highest': 2100, 'lowest': 1950, 'close_price': 2050}
        ])
        self.assertEqual(response.json()["pagination"]["count"], 2)
        self.mock_session.assert_called()
        mock_query.filter.assert_called_with(Position.symbol == u.validate_symbol('BTCUSDT'),
                                             Position.timestamp >= u.validate_date('2022-02-13 02:30:00'),
                                             Position.timestamp <= u.validate_date('2022-02-13 02:36:00'))
        mock_query.limit.assert_called_with(5)
        mock_query.offset.assert_called_with(0)
        mock_query.all.assert_called()

    @patch("csv_processor.api.get_db_session")
    def test_get_records_no_data(self, mock_get_db_session):
        # Test getting stock records when there are no records found
        mock_get_db_session.return_value = self.mock_session

        mock_query = MagicMock()
        self.mock_session.query.return_value = mock_query
        mock_query.count.return_value = 0

        response = self.client.get("/data")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {
            "data": {'result': 'No records found for symbol BTCUSDT '
                               'from 2022-02-13 02:30:00 to 2022-02-13 02:36:00'}
        })
        self.mock_session.assert_called()
        mock_query.filter.assert_called_with(Position.symbol == u.validate_symbol('BTCUSDT'),
                                             Position.timestamp >= u.validate_date('2022-02-13 02:30:00'),
                                             Position.timestamp <= u.validate_date('2022-02-13 02:36:00'))
        mock_query.limit.assert_called_with(5)
        mock_query.offset.assert_called_with(0)
        mock_query.all.assert_called()


if __name__ == "__main__":
    unittest.main()
