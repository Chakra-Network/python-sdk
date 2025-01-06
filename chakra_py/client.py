from typing import Any, Dict, Optional, Union

import pandas as pd
import requests
from colorama import Fore, Style
from tqdm import tqdm

from .exceptions import ChakraAPIError

BASE_URL = "https://api.chakra.dev".rstrip("/")


class Chakra:
    """Main client for interacting with the Chakra API.

    Provides a simple, unified interface for all Chakra operations including
    authentication, querying, and data manipulation.

    Example:
        >>> client = Chakra("DB_SESSION_KEY")
        >>> client.login()
        >>> df = client.execute("SELECT * FROM table")
        >>> client.push("new_table", df)
    """

    def __init__(
        self,
        db_session_key: str,
    ):
        """Initialize the Chakra client.

        Args:
            db_session_key: The DB session key to use - can be found in the Chakra Settings page
        """
        self._db_session_key = db_session_key
        self._token = None
        self._session = requests.Session()

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

    def _fetch_token(self, db_session_key: str) -> str:
        """Fetch a token from the Chakra API.

        Args:
            db_session_key: The DB session key to use

        Returns:
            The token to use for authentication
        """
        access_key_id, secret_access_key, username = db_session_key.split(":")

        response = self._session.post(
            f"{BASE_URL}/api/v1/servers",
            json={
                "accessKey": access_key_id,
                "secretKey": secret_access_key,
                "username": username,
            },
        )
        response.raise_for_status()
        return response.json()["token"]

    def login(self) -> None:
        """Set the authentication token for API requests.

        Raises:
            ValueError: If token doesn't start with 'DDB_'
        """
        print(f"\n{Fore.GREEN}Authenticating with Chakra DB...{Style.RESET_ALL}")

        with tqdm(
            total=100,
            desc="Authenticating",
            bar_format="{l_bar}{bar}| {n:.0f}%",
            colour="green",
        ) as pbar:

            pbar.update(30)
            pbar.set_description("Fetching token...")
            self.token = self._fetch_token(self._db_session_key)

            pbar.update(40)
            pbar.set_description("Token fetched")
            if not self.token.startswith("DDB_"):
                raise ValueError("Token must start with 'DDB_'")

            pbar.update(30)
            pbar.set_description("Authentication complete")

        print(f"{Fore.GREEN}✓ Successfully authenticated!{Style.RESET_ALL}\n")

    def execute(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as a pandas DataFrame."""
        if not self.token:
            raise ValueError("Authentication required")

        with tqdm(
            total=3,
            desc="Preparing query...",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} steps",
            colour="green",
        ) as pbar:

            pbar.set_description("Executing query...")
            try:
                response = self._session.post(
                    f"{BASE_URL}/api/v1/query", json={"sql": query}
                )
                response.raise_for_status()
            except Exception as e:
                self._handle_api_error(e)
            pbar.update(1)

            pbar.set_description("Processing results...")
            data = response.json()
            pbar.update(1)

            pbar.set_description("Building DataFrame...")
            df = pd.DataFrame(data["rows"], columns=data["columns"])
            pbar.update(1)

        print(f"{Fore.GREEN}✓ Query executed successfully!{Style.RESET_ALL}\n")
        return df

    def push(
        self,
        table_name: str,
        data: Union[pd.DataFrame, Dict[str, Any]],
        create_if_missing: bool = True,
        replace_if_exists: bool = False,
    ) -> None:
        """Push data to a table."""
        if not self.token:
            raise ValueError("Authentication required")

        if isinstance(data, pd.DataFrame):
            records = data.to_dict(orient="records")
            total_records = len(records)

            with tqdm(
                total=total_records,
                desc="Preparing data...",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} records",
                colour="green",
            ) as pbar:
                # TODO make this atomic https://duckdb.org/2024/09/25/changing-data-with-confidence-and-acid.html
                if replace_if_exists:
                    pbar.set_description(f"Replacing table...")
                    try:
                        response = self._session.post(
                            f"{BASE_URL}/api/v1/execute",
                            json={"sql": f"DROP TABLE IF EXISTS {table_name}"},
                        )
                        response.raise_for_status()
                    except Exception as e:
                        self._handle_api_error(e)

                if create_if_missing or replace_if_exists:
                    pbar.set_description("Creating table schema...")
                    columns = [
                        {"name": col, "type": self._map_pandas_to_duckdb_type(dtype)}
                        for col, dtype in data.dtypes.items()
                    ]
                    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
                    create_sql += ", ".join(
                        [f"{col['name']} {col['type']}" for col in columns]
                    )
                    create_sql += ")"

                    try:
                        response = self._session.post(
                            f"{BASE_URL}/api/v1/execute", json={"sql": create_sql}
                        )
                        response.raise_for_status()
                    except Exception as e:
                        self._handle_api_error(e)

                if records:
                    pbar.set_description(f"Preparing records...")
                    # kind of gross but create one large INSERT statement with multiple value sets
                    # batch is not well supported right now
                    values_list = []
                    for record in records:
                        values = [
                            str(v) if pd.notna(v) else None for v in record.values()
                        ]
                        value_set = (
                            "("
                            + ", ".join(
                                (
                                    f"'{v}'"
                                    if isinstance(v, str)
                                    else str(v) if v is not None else "NULL"
                                )
                                for v in values
                            )
                            + ")"
                        )
                        values_list.append(value_set)
                        pbar.update(1)

                    # Combine all value sets into a single INSERT statement
                    insert_sql = (
                        f"INSERT INTO {table_name} VALUES {', '.join(values_list)}"
                    )

                    pbar.set_description("Uploading data...")
                    try:
                        response = self._session.post(
                            f"{BASE_URL}/api/v1/execute",
                            json={"sql": insert_sql},
                        )
                        response.raise_for_status()
                    except Exception as e:
                        self._handle_api_error(e)

            print(
                f"{Fore.GREEN}✓ Successfully pushed {total_records} records to {table_name}!{Style.RESET_ALL}\n"
            )
        else:
            raise NotImplementedError("Dictionary input not yet implemented")

    def _map_pandas_to_duckdb_type(self, dtype) -> str:
        """Convert pandas dtype to DuckDB type.

        Args:
            dtype: Pandas dtype object

        Returns:
            str: Corresponding DuckDB type name
        """
        dtype_str = str(dtype)
        if "int" in dtype_str:
            return "BIGINT"
        elif "float" in dtype_str:
            return "DOUBLE"
        elif "bool" in dtype_str:
            return "BOOLEAN"
        elif "datetime" in dtype_str:
            return "TIMESTAMP"
        elif "timedelta" in dtype_str:
            return "INTERVAL"
        elif "object" in dtype_str:
            return "VARCHAR"
        else:
            return "VARCHAR"  # Default fallback

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
