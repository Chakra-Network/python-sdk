from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from chakra_py import Chakra


def test_client_initialization():
    """Test basic client initialization."""
    client = Chakra("access:secret:username")
    assert client.token is None
    assert isinstance(client._session, requests.Session)


@patch("requests.Session")
def test_auth_login(mock_session):
    """Test authentication login."""
    # Mock the token fetch response
    mock_response = Mock()
    mock_response.json.return_value = {"token": "DDB_test123"}
    mock_session.return_value.post.return_value = mock_response

    # Create a mock headers dictionary
    mock_session.return_value.headers = {}

    client = Chakra("access:secret:username")
    client.login()

    # Verify the token fetch request
    mock_session.return_value.post.assert_called_with(
        "https://api.chakra.dev/api/v1/servers",
        json={"accessKey": "access", "secretKey": "secret", "username": "username"},
    )

    assert client.token == "DDB_test123"
    # Verify the actual headers dictionary instead of the __getitem__ call
    assert mock_session.return_value.headers["Authorization"] == "Bearer DDB_test123"


@patch("requests.Session")
def test_query_execution(mock_session):
    """Test query execution and DataFrame conversion."""
    # Mock the token fetch response
    mock_auth_response = Mock()
    mock_auth_response.json.return_value = {"token": "DDB_test123"}

    # Mock the query response
    mock_query_response = Mock()
    mock_query_response.json.return_value = {
        "columns": ["id", "name"],
        "rows": [[1, "test"], [2, "test2"]],
    }

    mock_session.return_value.post.side_effect = [
        mock_auth_response,
        mock_query_response,
    ]

    # Initialize headers dictionary
    mock_session.return_value.headers = {}

    client = Chakra("access:secret:username")
    client.login()
    df = client.execute("SELECT * FROM test_table")

    # Verify DataFrame
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["id", "name"]
    assert len(df) == 2


@patch("uuid.uuid4")
@patch("requests.put")
@patch("requests.Session")
def test_data_push(mock_session, mock_requests_put, mock_uuid4):
    """Test data push functionality."""
    mock_uuid = "fake-uuid-1234"
    mock_uuid4.return_value = mock_uuid

    # Initialize headers dictionary
    mock_session.return_value.headers = {}

    # Mock responses
    mock_auth_response = Mock()
    mock_auth_response.json.return_value = {"token": "DDB_test123"}

    mock_presigned_response = Mock()
    mock_presigned_response.json.return_value = {
        "presignedUrl": "https://fake-s3-url.com",
        "key": "fake-s3-key",
    }
    mock_session.return_value.get.return_value = mock_presigned_response

    # Set up all mock responses in the correct order
    mock_session.return_value.post.side_effect = [
        mock_auth_response,  # For login
        Mock(status_code=200),  # For create database
        Mock(status_code=200),  # For create schema
        Mock(status_code=200),  # For create table
        Mock(status_code=200),  # For batch insert
        mock_presigned_response,  # For presigned URL
        Mock(status_code=200),  # For import
        Mock(status_code=200),  # For delete
    ]

    mock_requests_put.return_value = Mock(status_code=200)

    # Create test DataFrame
    df = pd.DataFrame({"id": [1, 2], "name": ["test1", "test2"]})

    # Create client and login first
    client = Chakra("access:secret:username")
    client.login()  # This will consume the first mock response

    # Now test pushing data
    client.push("test_database.test_schema.test_table", df)

    # 1. Verify the presigned URL GET request
    presigned_get_call = mock_session.return_value.get.call_args
    assert presigned_get_call[0][
        0
    ] == "https://api.chakra.dev/api/v1/presigned-upload?filename=test_database.test_schema.test_table_{}.parquet".format(
        mock_uuid
    )

    # 2. Verify the S3 upload was called with correct parameters
    mock_requests_put.assert_called_once()
    put_args = mock_requests_put.call_args
    assert put_args[0][0] == "https://fake-s3-url.com"
    assert put_args[1]["headers"] == {"Content-Type": "application/parquet"}
    assert "data" in put_args[1]

    # 3. Verify the create database request
    create_db_call = mock_session.return_value.post.call_args_list[1]
    assert create_db_call[0][0] == "https://api.chakra.dev/api/v1/databases"
    assert create_db_call[1]["json"] == {
        "name": "test_database",
        "insert_database": True,
    }

    # 4. Verify the create schema request
    create_schema_call = mock_session.return_value.post.call_args_list[2]
    assert create_schema_call[0][0] == "https://api.chakra.dev/api/v1/query"
    assert create_schema_call[1]["json"] == {
        "sql": "CREATE SCHEMA IF NOT EXISTS test_database.test_schema"
    }

    # 5. Verify the create table request
    create_table_call = mock_session.return_value.post.call_args_list[3]
    assert create_table_call[0][0] == "https://api.chakra.dev/api/v1/query"
    assert create_table_call[1]["json"] == {
        "sql": "CREATE TABLE IF NOT EXISTS test_database.test_schema.test_table (id BIGINT, name VARCHAR)"
    }

    # 6. Verify the import request
    import_call = mock_session.return_value.post.call_args_list[4]
    assert import_call[0][0] == "https://api.chakra.dev/api/v1/tables/s3_parquet_import"
    assert import_call[1]["json"] == {
        "table_name": "test_database.test_schema.test_table",
        "s3_key": "fake-s3-key",
    }

    # 7. Verify cleanup was called
    delete_call = mock_session.return_value.delete.call_args
    assert delete_call[0][0] == "https://api.chakra.dev/api/v1/files"
    assert delete_call[1]["json"] == {"fileName": "fake-s3-key"}
