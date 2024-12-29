from typing import Dict

class Auth:
    """Handles authentication with the Chakra API."""
    
    def __init__(self, client):
        self._client = client
    
    def login(self, token: str) -> None:
        """Set the authentication token for API requests.
        
        Args:
            token: The DDB token to use (format: 'DDB_xxxxx')
            
        Raises:
            ValueError: If token doesn't start with 'DDB_'
        """
        if not token.startswith('DDB_'):
            raise ValueError("Token must start with 'DDB_'")
        self._client.token = token
