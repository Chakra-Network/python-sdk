from unittest.mock import Mock, patch

import pandas as pd
import pytest

from chakra_sdk import ChakraClient


def test_client_initialization():
    """Test basic client initialization."""
    client = ChakraClient()
    assert client.base_url == "http://api.chakra.dev"
    assert client.token is None


def test_client_token_setting():
    """Test token setting and header updates."""
    client = ChakraClient()
    test_token = "DDB_test123"
    client.token = test_token
    assert client.token == test_token
    assert client._session.headers["Authorization"] == f"Bearer {test_token}"


def test_auth_login():
    """Test authentication login with token validation."""
    client = ChakraClient()

    # Test valid token
    valid_token = "DDB_test123"
    client.auth.login(valid_token)
    assert client.token == valid_token
    assert client._session.headers["Authorization"] == f"Bearer {valid_token}"

    # Test invalid token format
    with pytest.raises(ValueError, match="Token must start with 'DDB_'"):
        client.auth.login("invalid_token")


@patch("requests.Session")
def test_query_execution(mock_session):
    """Test query execution and DataFrame conversion."""
    client = ChakraClient()
    client.auth.login("DDB_test123")

    # Mock response data
    mock_response = Mock()
    mock_response.json.return_value = {
        "columns": ["id", "name"],
        "rows": [[1, "test"], [2, "test2"]],
    }
    mock_session.return_value.post.return_value = mock_response

    # Test query execution
    df = client.query.execute("SELECT * FROM test_table")

    # Verify request
    mock_session.return_value.post.assert_called_with(
        "http://api.chakra.dev/api/v1/query", json={"sql": "SELECT * FROM test_table"}
    )

    # Verify DataFrame
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["id", "name"]
    assert len(df) == 2

    # Test authentication check
    client = ChakraClient()  # New client without token
    with pytest.raises(ValueError, match="Authentication required"):
        client.query.execute("SELECT * FROM test_table")


@patch("requests.Session")
def test_data_push(mock_session):
    """Test data push functionality."""
    client = ChakraClient()
    client.auth.login("DDB_test123")

    # Create test DataFrame
    df = pd.DataFrame({"id": [1, 2], "name": ["test1", "test2"]})

    # Mock responses
    mock_response = Mock()
    mock_response.status_code = 200
    mock_session.return_value.post.return_value = mock_response

    # Test pushing data
    client.data.push("test_table", df)

    # Verify create table request
    create_call = mock_session.return_value.post.call_args_list[0]
    assert create_call[0][0] == "http://api.chakra.dev/api/v1/execute"
    assert "CREATE TABLE IF NOT EXISTS test_table" in create_call[1]["json"]["sql"]

    # Verify batch insert request
    insert_call = mock_session.return_value.post.call_args_list[1]
    assert insert_call[0][0] == "http://api.chakra.dev/api/v1/execute/batch"
    assert len(insert_call[1]["json"]["statements"]) == 2

    # Test authentication check
    client = ChakraClient()  # New client without token
    with pytest.raises(ValueError, match="Authentication required"):
        client.data.push("test_table", df)

    # Test dictionary input not implemented
    with pytest.raises(NotImplementedError):
        client.data.push("test_table", {"key": "value"})
