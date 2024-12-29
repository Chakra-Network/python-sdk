"""Chakra SDK Client.

This module provides the main client interface for interacting with the Chakra API.
"""

from typing import Optional, Union

import pandas as pd

from .api.auth import (
    ChakraAuthError,
    SessionCreationError,
    create_database_session,
    validate_session,
)


class ChakraClient:
    """Main client for interacting with the Chakra API.

    This client supports both credential-based authentication (recommended)
    and direct token authentication (legacy).

    Args:
        api_base_url: Base URL for the Chakra API
        access_key_id: Optional access key ID for authentication
        secret_key: Optional secret key for authentication
        username: Optional username for the session (defaults to "default")
        token: Optional token for direct authentication (legacy)

    Raises:
        SessionCreationError: If session creation fails using credentials
        ValueError: If neither credentials nor token are provided
    """

    def __init__(
        self,
        api_base_url: str,
        access_key_id: Optional[str] = None,
        secret_key: Optional[str] = None,
        username: str = "default",
        token: Optional[str] = None,
    ):
        self.api_base_url = api_base_url.rstrip("/")
        self.token: Optional[str] = None

        # Credential-based authentication (recommended)
        if access_key_id and secret_key:
            try:
                self.token = create_database_session(
                    api_base_url=self.api_base_url,
                    access_key_id=access_key_id,
                    secret_key=secret_key,
                    username=username,
                )
            except SessionCreationError as e:
                raise SessionCreationError(
                    f"Failed to create session with credentials: {str(e)}"
                ) from e

        # Legacy token-based authentication
        elif token:
            if not validate_session(self.api_base_url, token):
                raise ChakraAuthError("Invalid token provided")
            self.token = token

        else:
            raise ValueError(
                "Either credentials (access_key_id and secret_key) or token must be provided"
            )

    def query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as a pandas DataFrame.

        Args:
            sql: SQL query to execute

        Returns:
            pandas.DataFrame: Query results

        Raises:
            ChakraAuthError: If no valid token is available
        """
        if not self.token:
            raise ChakraAuthError("No valid token available")

        # TODO: Implement query functionality
        raise NotImplementedError("Query functionality not implemented yet")

    def push_data(self, table_name: str, data: Union[pd.DataFrame, dict]) -> None:
        """Push data to a new or existing table.

        Args:
            table_name: Name of the table to push data to
            data: Data to push (pandas DataFrame or dictionary)

        Raises:
            ChakraAuthError: If no valid token is available
        """
        if not self.token:
            raise ChakraAuthError("No valid token available")

        # TODO: Implement data push functionality
        raise NotImplementedError("Data push functionality not implemented yet")
