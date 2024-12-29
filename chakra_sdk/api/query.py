from typing import Optional, Dict, Any
import pandas as pd

class Query:
    """Handles data querying operations."""
    
    def __init__(self, client):
        self._client = client
    
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
        if not self._client.token:
            raise ValueError("Authentication required. Call client.auth.login() first.")
            
        response = self._client._session.post(
            f"{self._client.base_url}/api/v1/query",
            json={"sql": query}
        )
        response.raise_for_status()
        
        data = response.json()
        # Convert the columnar format to DataFrame
        return pd.DataFrame(data['rows'], columns=data['columns'])
