from typing import Any, Dict, Optional, Union

import pandas as pd
import requests

from .api.auth import Auth
from .api.data import Data
from .api.query import Query
from .exceptions import ChakraAPIError


class ChakraClient:
    """Main client for interacting with the Chakra API."""

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
