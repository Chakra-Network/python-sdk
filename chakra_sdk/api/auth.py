"""Authentication module for Chakra SDK.

This module handles database session creation and management using access key credentials.
"""

from typing import Optional

import requests


class ChakraAuthError(Exception):
    """Base exception for authentication errors."""

    pass


class SessionCreationError(ChakraAuthError):
    """Raised when session creation fails."""

    pass


class SessionValidationError(ChakraAuthError):
    """Raised when session validation fails."""

    pass


def create_database_session(
    api_base_url: str, access_key_id: str, secret_key: str, username: str = "default"
) -> str:
    """Create a new database session using access key credentials.

    Args:
        api_base_url: Base URL for the Chakra API
        access_key_id: Access key ID for authentication
        secret_key: Secret key for authentication
        username: Username for the session (defaults to "default")

    Returns:
        str: Session token for subsequent API calls

    Raises:
        SessionCreationError: If session creation fails
    """
    payload = {
        "accessKey": access_key_id,
        "secretKey": secret_key,
        "username": username,
    }

    try:
        response = requests.post(f"{api_base_url}/api/v1/servers", json=payload)
        if not response.ok:
            raise SessionCreationError(
                f"Failed to create database session: HTTP {response.status_code}"
            )
        data = response.json()
        if "token" not in data:
            raise SessionCreationError("No token in response")
        return data["token"]
    except requests.exceptions.RequestException as e:
        raise SessionCreationError(
            f"Failed to create database session: {str(e)}"
        ) from e


def validate_session(api_base_url: str, token: str) -> bool:
    """Validate an existing database session token.

    Args:
        api_base_url: Base URL for the Chakra API
        token: Session token to validate

    Returns:
        bool: True if session is valid, False otherwise
    """
    try:
        response = requests.get(
            f"{api_base_url}/api/v1/servers/status",
            headers={"Authorization": f"Bearer {token}"},
        )
        if response.status_code == 401:
            return False
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException:
        return False


def close_session(api_base_url: str, token: str) -> None:
    """Close an existing database session.

    Note: Currently unused as shutdowns are handled by the server automatically.

    Args:
        api_base_url: Base URL for the Chakra API
        token: Session token to close

    Raises:
        SessionValidationError: If session closure fails
    """
    try:
        response = requests.post(
            f"{api_base_url}/api/v1/servers/shutdown",
            headers={"Authorization": f"Bearer {token}"},
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise SessionValidationError(
            f"Failed to close database session: {str(e)}"
        ) from e
