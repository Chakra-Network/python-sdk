"""Tests for the authentication module."""
import pytest
import requests

from chakra_sdk.api.auth import (
    ChakraAuthError,
    SessionCreationError,
    create_database_session,
    validate_session,
    close_session,
)


@pytest.fixture(autouse=True)
def mock_successful_response(monkeypatch):
    """Mock successful API responses."""
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
def mock_failed_response(monkeypatch):
    """Mock failed API responses."""
    class MockResponse:
        def __init__(self):
            self.ok = False
            self.status_code = 401

        def raise_for_status(self):
            raise requests.exceptions.HTTPError("Unauthorized")

    def mock_post(*args, **kwargs):
        return MockResponse()

    def mock_get(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests, "get", mock_get)


def test_create_database_session_success(mock_successful_response):
    """Test successful database session creation."""
    token = create_database_session(
        api_base_url="http://api.test",
        access_key_id="test_key",
        secret_key="test_secret",
        username="test_user"
    )
    assert token == "test_token"


def test_create_database_session_failure(mock_failed_response):
    """Test failed database session creation."""
    with pytest.raises(SessionCreationError):
        create_database_session(
            api_base_url="http://api.test",
            access_key_id="invalid_key",
            secret_key="invalid_secret"
        )


def test_validate_session_success(mock_successful_response):
    """Test successful session validation."""
    assert validate_session("http://api.test", "test_token") is True


def test_validate_session_failure(mock_failed_response):
    """Test failed session validation."""
    assert validate_session("http://api.test", "invalid_token") is False


def test_close_session_success(mock_successful_response):
    """Test successful session closure."""
    close_session("http://api.test", "test_token")  # Should not raise


def test_close_session_failure(mock_failed_response):
    """Test failed session closure."""
    with pytest.raises(ChakraAuthError):
        close_session("http://api.test", "invalid_token")
