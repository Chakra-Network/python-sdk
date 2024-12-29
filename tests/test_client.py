"""Tests for the ChakraClient class."""
import pytest
import requests

from chakra_sdk.client import ChakraClient
from chakra_sdk.api.auth import ChakraAuthError, SessionCreationError


@pytest.fixture(autouse=True)
def mock_auth_success(monkeypatch):
    """Mock successful authentication."""
    class MockResponse:
        def __init__(self):
            self.ok = True
            self._json = {"token": "test_token"}
            self.status_code = 200

        def json(self):
            return self._json

        def raise_for_status(self):
            pass

    def mock_request(*args, **kwargs):
        return MockResponse()

    # Mock all requests methods
    monkeypatch.setattr(requests, "post", mock_request)
    monkeypatch.setattr(requests, "get", mock_request)
    monkeypatch.setattr(requests, "put", mock_request)
    monkeypatch.setattr(requests, "delete", mock_request)


@pytest.fixture
def mock_auth_failure(monkeypatch):
    """Mock failed authentication."""
    class MockFailedResponse:
        def __init__(self):
            self.ok = False
            self.status_code = 401

        def json(self):
            return {"error": "Authentication failed"}

        def raise_for_status(self):
            raise requests.exceptions.HTTPError("401 Client Error: Unauthorized")

    def mock_request(*args, **kwargs):
        return MockFailedResponse()

    # Override the successful response fixture for this test
    monkeypatch.setattr(requests, "post", mock_request)
    monkeypatch.setattr(requests, "get", mock_request)
    monkeypatch.setattr(requests, "put", mock_request)
    monkeypatch.setattr(requests, "delete", mock_request)


def test_client_init_with_credentials_success(mock_auth_success):
    """Test successful client initialization with credentials."""
    client = ChakraClient(
        api_base_url="http://api.test",
        access_key_id="test_key",
        secret_key="test_secret"
    )
    assert client.token == "test_token"


def test_client_init_with_credentials_failure(mock_auth_failure):
    """Test failed client initialization with credentials."""
    with pytest.raises(SessionCreationError):
        ChakraClient(
            api_base_url="http://api.test",
            access_key_id="invalid_key",
            secret_key="invalid_secret"
        )


def test_client_init_with_token_success(mock_auth_success):
    """Test successful client initialization with token."""
    client = ChakraClient(
        api_base_url="http://api.test",
        token="test_token"
    )
    assert client.token == "test_token"


def test_client_init_with_token_failure(mock_auth_failure):
    """Test failed client initialization with invalid token."""
    with pytest.raises(ChakraAuthError):
        ChakraClient(
            api_base_url="http://api.test",
            token="invalid_token"
        )


def test_client_init_missing_auth():
    """Test client initialization with no authentication."""
    with pytest.raises(ValueError):
        ChakraClient(api_base_url="http://api.test")
