import json
import unittest
from unittest.mock import MagicMock, patch, call

import psycopg2
import requests

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import extract_load


class TestGetCryptoData(unittest.TestCase):

    @patch('extract_load.requests.get')
    def test_get_crypto_data_success_returns_json(self, mock_get):
        expected = [{"id": "bitcoin", "current_price": 50000}]
        mock_response = MagicMock()
        mock_response.json.return_value = expected
        mock_get.return_value = mock_response

        result = extract_load.get_crypto_data()

        self.assertEqual(result, expected)
        mock_response.raise_for_status.assert_called_once()

    @patch('extract_load.requests.get')
    def test_get_crypto_data_http_error_returns_none(self, mock_get):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")
        mock_get.return_value = mock_response

        result = extract_load.get_crypto_data()

        self.assertIsNone(result)


class TestLoadRawData(unittest.TestCase):

    @patch('extract_load.psycopg2.connect')
    def test_load_raw_data_inserts_json_and_commits(self, mock_connect):
        data = [{"id": "bitcoin", "current_price": 50000}]

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        result = extract_load.load_raw_data(data)

        self.assertTrue(result)
        mock_cur.execute.assert_called_once_with(
            "INSERT INTO Staging_API_Response (RawJSON) VALUES (%s)",
            (json.dumps(data),)
        )
        mock_conn.commit.assert_called_once()

    @patch('extract_load.psycopg2.connect')
    def test_load_raw_data_db_error_returns_false(self, mock_connect):
        data = [{"id": "bitcoin"}]

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur
        mock_cur.execute.side_effect = psycopg2.DatabaseError("insert failed")

        result = extract_load.load_raw_data(data)

        self.assertFalse(result)
        mock_conn.close.assert_called_once()


class TestTriggerTransformation(unittest.TestCase):

    @patch('extract_load.psycopg2.connect')
    def test_trigger_transformation_calls_stored_proc(self, mock_connect):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        result = extract_load.trigger_transformation()

        self.assertTrue(result)
        mock_cur.execute.assert_called_once_with("CALL sp_ParseRawData();")
        mock_conn.commit.assert_called_once()

    @patch('extract_load.psycopg2.connect')
    def test_trigger_transformation_db_error_returns_false(self, mock_connect):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur
        mock_cur.execute.side_effect = psycopg2.DatabaseError("proc failed")

        result = extract_load.trigger_transformation()

        self.assertFalse(result)


class TestLogPipelineStart(unittest.TestCase):

    @patch('extract_load.psycopg2.connect')
    def test_log_pipeline_start_returns_run_id(self, mock_connect):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchone.return_value = (123,)

        result = extract_load.log_pipeline_start()

        self.assertEqual(result, 123)
        mock_conn.commit.assert_called_once()


class TestLogPipelineEnd(unittest.TestCase):

    @patch('extract_load.psycopg2.connect')
    def test_log_pipeline_end_noop_when_run_id_none(self, mock_connect):
        extract_load.log_pipeline_end(None, "FAILED", "some error")

        mock_connect.assert_not_called()


class TestRunPipeline(unittest.TestCase):

    @patch('extract_load.log_pipeline_end')
    @patch('extract_load.trigger_transformation')
    @patch('extract_load.load_raw_data')
    @patch('extract_load.get_crypto_data')
    @patch('extract_load.log_pipeline_start')
    def test_run_pipeline_happy_path_calls_log_start_load_transform_log_end_success(
        self,
        mock_log_start,
        mock_get_data,
        mock_load,
        mock_transform,
        mock_log_end,
    ):
        mock_log_start.return_value = 42
        mock_get_data.return_value = [{"id": "bitcoin"}]
        mock_load.return_value = True
        mock_transform.return_value = True

        extract_load.run_pipeline()

        mock_log_start.assert_called_once()
        mock_get_data.assert_called_once()
        mock_load.assert_called_once_with([{"id": "bitcoin"}])
        mock_transform.assert_called_once()
        mock_log_end.assert_called_once_with(42, "SUCCESS", None)

    @patch('extract_load.log_pipeline_end')
    @patch('extract_load.get_crypto_data')
    @patch('extract_load.log_pipeline_start')
    def test_run_pipeline_no_data_logs_failed_with_message(
        self,
        mock_log_start,
        mock_get_data,
        mock_log_end,
    ):
        mock_log_start.return_value = 7
        mock_get_data.return_value = None

        extract_load.run_pipeline()

        mock_log_end.assert_called_once_with(7, "FAILED", "No data fetched")


if __name__ == '__main__':
    unittest.main()
