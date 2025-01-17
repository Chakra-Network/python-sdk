from typing import Any, Dict, Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from colorama import Fore, Style
from tqdm import tqdm
from io import BytesIO
import uuid
import time
from .exceptions import ChakraAPIError

BASE_URL = "https://api.chakra.dev".rstrip("/")

# Add constants at the top
DEFAULT_BATCH_SIZE = 1000
TOKEN_PREFIX = "DDB_"


def ensure_authenticated(func):
    """Decorator to ensure the client is authenticated before executing a method."""

    def wrapper(self, *args, **kwargs):
        max_attempts = 3
        attempt = 0

        while attempt < max_attempts:
            if not self.token:
                self.login()
            try:
                return func(self, *args, **kwargs)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    attempt += 1
                    print(
                        f"Attempt {attempt} failed with 401. Stale token. Attempting login..."
                    )
                    self.login()
                else:
                    raise
        raise ChakraAPIError("Failed to authenticate after 3 attempts.")

    return wrapper


class Chakra:
    """Main client for interacting with the Chakra API.

    Provides a simple, unified interface for all Chakra operations including
    authentication, querying, and data manipulation.

    Example:
        >>> client = Chakra("DB_SESSION_KEY")
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

    def _create_table_schema(
        self, table_name: str, data: pd.DataFrame, pbar: tqdm
    ) -> None:
        """Create table schema if it doesn't exist."""
        pbar.set_description("Creating table schema...")
        columns = [
            {"name": col, "type": self._map_pandas_to_duckdb_type(dtype)}
            for col, dtype in data.dtypes.items()
        ]
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        create_sql += ", ".join(f"{col['name']} {col['type']}" for col in columns)
        create_sql += ")"

        response = self._session.post(
            f"{BASE_URL}/api/v1/query", json={"sql": create_sql}
        )
        response.raise_for_status()

    def _replace_existing_table(self, table_name: str, pbar: tqdm) -> None:
        """Drop existing table if replace_if_exists is True."""
        pbar.set_description(f"Replacing table...")
        response = self._session.post(
            f"{BASE_URL}/api/v1/query",
            json={"sql": f"DROP TABLE IF EXISTS {table_name}"},
        )
        response.raise_for_status()

    def _process_batch(
        self, table_name: str, batch: list, batch_number: int, pbar: tqdm
    ) -> None:
        """Process and upload a single batch of records."""
        # Create placeholders for the batch
        value_placeholders = "(" + ", ".join(["?" for _ in batch[0]]) + ")"
        batch_placeholders = ", ".join([value_placeholders for _ in batch])
        insert_sql = f"INSERT INTO {table_name} VALUES {batch_placeholders}"

        # Flatten parameters for this batch
        parameters = [
            str(value) if pd.notna(value) else "NULL"
            for record in batch
            for value in record.values()
        ]

        pbar.set_description(f"Uploading batch {batch_number}...")
        response = self._session.post(
            f"{BASE_URL}/api/v1/query",
            json={"sql": insert_sql, "parameters": parameters},
        )
        response.raise_for_status()
    
    def _push_to_presigned_url(self, presigned_url: str, df: pd.DataFrame) -> None:
        """
        Upload a pandas DataFrame to S3 as a parquet file using a presigned URL.
        
        Args:
            df: The pandas DataFrame to upload
            presigned_url: The S3 presigned URL for uploading
            
        Returns:
            bool: True if upload was successful, False otherwise
        """
        

    @ensure_authenticated
    def push(
        self,
        table_name: str,
        data: pd.DataFrame,
        create_if_missing: bool = True,
        replace_if_exists: bool = False,
        # batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        """Push data to a table."""
        if not self.token:
            raise ValueError("Authentication required")
        
        total_records = len(data)

        with tqdm(
            total=total_records,
            desc="Preparing data...",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} records",
            colour="green",
        ) as pbar:
            try:
                if replace_if_exists:
                    self._replace_existing_table(table_name, pbar)

                if create_if_missing or replace_if_exists:
                    self._create_table_schema(table_name, data, pbar)

                uuid_str = str(uuid.uuid4())
                filename = f"{table_name}_{uuid_str}.parquet"
                response = self._session.get(
                    f"{BASE_URL}/api/v1/presigned-upload?filename={filename}",
                )
                response.raise_for_status()
                response_json = response.json()
                presigned_url = response_json["presignedUrl"]

                print(f"Response JSON: {response_json}")
                print(f"Presigned URL: {presigned_url}")

                base_file_url = presigned_url.split("?")[0]
                print(f"Base file URL: {base_file_url}")

                # Create a temporary file to write the parquet data
                temp_file = BytesIO()
                data.to_parquet(temp_file, engine='pyarrow', compression='zstd')
                temp_file.seek(0)

                start_time = time.time()

                # Upload the temporary file using requests
                response = requests.put(
                    presigned_url,
                    data=temp_file.getvalue(),
                    headers={'Content-Type': 'application/parquet'}
                )
                response.raise_for_status()

                print(f"Upload response: {response.status_code}")

                temp_file.close()

                response = self._session.post(
                    f"{BASE_URL}/api/v1/tables/s3_parquet_import",
                    json={
                        "table_name": table_name,
                        "s3_key": f"uploads/{filename}",
                    },
                )
                response.raise_for_status()

                end_time = time.time()
                print(f"Time taken: {end_time - start_time} seconds")

                print(f"Upload response: {response.status_code}")

            except Exception as e:
                self._handle_api_error(e)

        print(
            f"{Fore.GREEN}✓ Successfully pushed {total_records} records to {table_name}!{Style.RESET_ALL}\n"
        )

    def login(self) -> None:
        """Set the authentication token for API requests."""
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
            if not self.token.startswith(TOKEN_PREFIX):
                raise ValueError(f"Token must start with '{TOKEN_PREFIX}'")

            pbar.update(30)
            pbar.set_description("Authentication complete")

        print(f"{Fore.GREEN}✓ Successfully authenticated!{Style.RESET_ALL}\n")

    # HACK: this is a hack to get around the fact that the duckdb go doesn't support positional parameters
    def __query_has_positional_parameters(self, query: str) -> bool:
        """Check if the query has positional parameters."""
        return "$1" in query

    def __replace_position_parameters_with_autoincrement(
        self, query: str, parameters: list
    ) -> str:
        """Replace positional parameters with autoincrement."""
        if len(parameters) > 9:
            raise ValueError(
                "Chakra DB does not support more than 8 positional parameters"
            )
        # find all $1, $2, $3, etc. and replace with ?, ?, ?, etc.
        new_query = query
        for i in range(len(parameters)):
            new_query = new_query.replace(f"${i+1}", f"?")

        # explode the parameters into a single list with duplicates aligned
        new_parameters = []
        # iterate over query, find the relevant index in parameters, then add the value to new_parameters
        for i in range(len(query)):
            if query[i] == "$":
                index = int(query[i + 1])
                # duckdb uses 1-indexed parameters, so we need to subtract 1
                new_parameters.append(parameters[index - 1])

        return new_query, new_parameters

    @ensure_authenticated
    def execute(self, query: str, parameters: list = []) -> pd.DataFrame:
        """Execute a query and return results as a pandas DataFrame."""
        if not self.token:
            raise ValueError("Authentication required")

        with tqdm(
            total=3,
            desc="Preparing query...",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} steps",
            colour="green",
        ) as pbar:
            if self.__query_has_positional_parameters(query):
                query, parameters = (
                    self.__replace_position_parameters_with_autoincrement(
                        query, parameters
                    )
                )
            pbar.set_description("Executing query...")
            response = self._session.post(
                f"{BASE_URL}/api/v1/query",
                json={"sql": query, "parameters": parameters},
            )
            response.raise_for_status()
            pbar.update(1)

            pbar.set_description("Processing results...")
            data = response.json()
            pbar.update(1)

            pbar.set_description("Building DataFrame...")
            df = pd.DataFrame(data["rows"], columns=data["columns"])
            pbar.update(1)

        print(f"{Fore.GREEN}✓ Query executed successfully!{Style.RESET_ALL}\n")
        return df

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
