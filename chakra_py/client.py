from typing import Any, Dict, Optional, Union

import pandas as pd
import requests

from .api.auth import Auth
from .api.data import Data
from .api.query import Query
from .exceptions import ChakraAPIError


class ChakraClient:
    """Main client for interacting with the Chakra API.
    
    Provides a simple, unified interface for all Chakra operations including
    authentication, querying, and data manipulation. Similar to other modern
    Python SDKs like exa-py, all operations are available directly from the
    client instance.

    Example:
        >>> client = ChakraClient()
        >>> client.login("DDB_your_token")
        >>> df = client.execute("SELECT * FROM table")
        >>> client.push("new_table", df)
    """

    def __init__(
        self, base_url: str = "http://api.chakra.dev", token: Optional[str] = None
    ):
        """Initialize the Chakra client.

        Args:
            base_url: The base URL for the Chakra API
            token: Optional authentication token
        """
        self.base_url = base_url.rstrip("/")
        self._token = token
        self._session = requests.Session()

        # Initialize API components
        self.auth = Auth(self)
        self.query = Query(self)
        self.data = Data(self)

    @property
    def token(self) -> Optional[str]:
        return self._token

    @token.setter
    def token(self, value: str):
        self._token = value
        if value:
            self._session.headers.update({"Authorization": f"Bearer {value}"})
        else:
            self._session.headers.pop("Authorization", None)

    def login(self, token: str) -> None:
        """Set the authentication token for API requests.

        Args:
            token: The DDB token to use (format: 'DDB_xxxxx')

        Raises:
            ValueError: If token doesn't start with 'DDB_'
        """
        return self.auth.login(token)

    def execute(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as a pandas DataFrame.

        Args:
            query: The SQL query string to execute

        Returns:
            pandas.DataFrame containing the query results

        Raises:
            requests.exceptions.HTTPError: If the query fails
            ValueError: If not authenticated
        """
        return self.query.execute(query)

    def push(
        self,
        table_name: str,
        data: Union[pd.DataFrame, Dict[str, Any]],
        create_if_missing: bool = True,
    ) -> None:
        """Push data to a table.

        Args:
            table_name: Name of the target table
            data: DataFrame or dictionary containing the data to push
            create_if_missing: Whether to create the table if it doesn't exist

        Raises:
            requests.exceptions.HTTPError: If the push operation fails
            ValueError: If not authenticated
        """
        return self.data.push(table_name, data, create_if_missing)

    def _handle_api_error(self, e: Exception) -> None:
        """Handle API errors consistently.

        Args:
            e: The original exception

        Raises:
            ChakraAPIError: Enhanced error with API response details
        """
        if hasattr(e, "response") and hasattr(e.response, "json"):
            try:
                error_msg = e.response.json().get("error", str(e))
                raise ChakraAPIError(error_msg, e.response) from e
            except ValueError:  # JSON decoding failed
                raise ChakraAPIError(str(e), e.response) from e
        raise e  # Re-raise original exception if not an API error
