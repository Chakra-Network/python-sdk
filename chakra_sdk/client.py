"""Chakra SDK Client.

This module provides the main client interface for interacting with the Chakra API.
"""

from typing import Union

import pandas as pd

from .api.auth import (
    ChakraAuthError,
    SessionCreationError,
    create_database_session,
)


class ChakraClient:
    """Main client for interacting with the Chakra API.

    Args:
        api_base_url: Base URL for the Chakra API
        access_key_id: Access key ID for authentication
        secret_key: Secret key for authentication
        username: Optional username for the session (defaults to "default")

    Raises:
        SessionCreationError: If session creation fails using credentials
    """

    def __init__(
        self,
        api_base_url: str,
        access_key_id: str,
        secret_key: str,
        username: str = "default",
    ):
        self.api_base_url = api_base_url.rstrip("/")
        self.token: str

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
