import os
import tempfile
import uuid
from typing import Any, Dict, Optional, Union

import pandas as pd
import requests
from colorama import Fore, Style
from tqdm import tqdm

from .exceptions import ChakraAPIError

BASE_URL = "https://api.chakra.dev".rstrip("/")

DEFAULT_BATCH_SIZE = 1000
TOKEN_PREFIX = "DDB_"


__version__ = "1.0.19"
__all__ = ["Chakra"]

BANNER = rf"""{Fore.GREEN}
 _____ _           _               ________   __
/  __ \ |         | |              | ___ \ \ / /
| /  \/ |__   __ _| | ___ __ __ _  | |_/ /\ V / 
| |   | '_ \ / _` | |/ / '__/ _` | |  __/  \ /  
| \__/\ | | | (_| |   <| | | (_| | | |     | |  
 \____/_| |_|\__,_|_|\_\_|  \__,_| \_|     \_/  
{Style.RESET_ALL}
                                   
Python SDK v{__version__}
"""


class ProgressFileWrapper:
    def __init__(self, file, total_size, tdqm):
        self.file = file
        self.progress_bar = tdqm
        self._len = total_size

    def read(self, size=-1):
        data = self.file.read(size)
        if data:
            self.progress_bar.update(len(data))
        return data

    def __len__(self):
        return self._len


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
        quiet: bool = False,
    ):
        """Initialize the Chakra client.

        Args:
            db_session_key: The DB session key to use - can be found in the Chakra Settings page
            quiet: If True, suppresses all stdout messages (default: False)
        """
        self._db_session_key = db_session_key
        self._token = None
        self._session = requests.Session()
        self._quiet = quiet

        if not quiet:
            print(BANNER.format(version=__version__))

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

    def _create_database_and_schema(self, table_name: str, pbar: tqdm) -> None:
        """Create database, schema, and table if they don't exist."""
        pbar.set_description("Creating database if it doesn't exist...")
        [database_name, schema_name, _] = table_name.split(".")
        response = self._session.post(
            f"{BASE_URL}/api/v1/databases",
            json={"name": database_name},
        )
        if response.status_code != 409:
            # only raise error if the database doesn't already exist
            response.raise_for_status()

        pbar.set_description(f"Creating schema {schema_name} if it doesn't exist...")

        create_sql = f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}"
        response = self._session.post(
            f"{BASE_URL}/api/v1/query", json={"sql": create_sql}
        )
        response.raise_for_status()

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

    def _request_presigned_url(self, file_name: str) -> dict:
        """Request a presigned URL for the upload."""
        response = self._session.get(
            f"{BASE_URL}/api/v1/presigned-upload?filename={file_name}",
        )
        response.raise_for_status()
        return response.json()

    def _upload_parquet_using_presigned_url(
        self, presigned_url: str, file: str, file_size: int, pbar: tqdm
    ) -> None:
        """Upload a parquet file to S3 using a presigned URL."""
        progress_wrapper = ProgressFileWrapper(file, file_size, pbar)

        pbar.set_description("Uploading data...")
        response = requests.put(
            presigned_url,
            data=progress_wrapper,
            headers={"Content-Type": "application/parquet"},
        )
        response.raise_for_status()

    def _import_data_from_presigned_url(self, table_name: str, s3_key: str) -> None:
        """Import data from a presigned URL into a table."""
        response = self._session.post(
            f"{BASE_URL}/api/v1/tables/s3_parquet_import",
            json={"table_name": table_name, "s3_key": s3_key},
        )
        response.raise_for_status()

    def _import_data_from_append_only_dedupe_presigned_url(
        self, table_name: str, s3_key: str, primary_key_columns: list[str]
    ) -> None:
        """Import data from a presigned URL into a table."""
        response = self._session.post(
            f"{BASE_URL}/api/v1/tables/s3_parquet_import_append_only_dedupe",
            json={
                "table_name": table_name,
                "s3_key": s3_key,
                "primary_key_columns": primary_key_columns,
            },
        )
        response.raise_for_status()

    def _delete_file_from_s3(self, s3_key: str) -> None:
        """Delete a file from S3."""
        response = self._session.delete(
            f"{BASE_URL}/api/v1/files",
            json={"fileName": s3_key},
        )
        response.raise_for_status()

    def _print(self, message: str) -> None:
        """Print a message if quiet mode is not enabled."""
        if not self._quiet:
            print(message)

    @ensure_authenticated
    def push(
        self,
        table_name: str,
        data: pd.DataFrame,
        create_if_missing: bool = True,
        replace_if_exists: bool = False,
        dedupe_on_append: bool = False,
        primary_key_columns: list[str] = [],
    ) -> None:
        """Push data to a table."""
        if not self.token:
            raise ValueError("Authentication required")

        total_records = len(data)

        with tempfile.NamedTemporaryFile() as temp_file:
            data.to_parquet(
                temp_file.name,
                engine="pyarrow",
                compression="zstd",
                index=False,
            )
            file_size = os.path.getsize(temp_file.name)

            with tqdm(
                total=file_size + 2,
                desc="Uploading data...",
                bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                colour="green",
                unit="B",
                unit_scale=True,
                disable=self._quiet,
            ) as pbar:
                try:
                    if create_if_missing or replace_if_exists:
                        self._create_database_and_schema(table_name, pbar)

                    if replace_if_exists:
                        self._replace_existing_table(table_name, pbar)

                    if create_if_missing or replace_if_exists:
                        self._create_table_schema(table_name, data, pbar)

                    # Request a presigned URL for the upload
                    uuid_str = str(uuid.uuid4())
                    filename = f"{table_name}_{uuid_str}.parquet"
                    response = self._request_presigned_url(filename)
                    presigned_url = response["presignedUrl"]
                    s3_key = response["key"]

                    # Upload the data to the presigned URL
                    temp_file.seek(0)
                    self._upload_parquet_using_presigned_url(
                        presigned_url, temp_file, file_size, pbar
                    )

                    # Import the data into the warehouse from the presigned URL
                    pbar.set_description("Importing data into warehouse...")
                    if dedupe_on_append:
                        self._import_data_from_append_only_dedupe_presigned_url(
                            table_name, s3_key, primary_key_columns
                        )
                    else:
                        self._import_data_from_presigned_url(table_name, s3_key)
                    pbar.update(1)

                    # Clean up the data that was previously uploaded
                    pbar.set_description("Cleaning up...")
                    self._delete_file_from_s3(s3_key)
                    pbar.update(1)

                    pbar.set_description("Data import finished.")

                except Exception as e:
                    self._handle_api_error(e)

        self._print(
            f"{Fore.GREEN}✓ Successfully pushed {total_records} records to {table_name}!{Style.RESET_ALL}\n"
        )

    def login(self) -> None:
        """Set the authentication token for API requests."""
        self._print(f"\n{Fore.GREEN}Authenticating with Chakra DB...{Style.RESET_ALL}")

        with tqdm(
            total=100,
            desc="Authenticating",
            bar_format="{l_bar}{bar}| {n:.0f}%",
            colour="green",
            disable=self._quiet,
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

        self._print(f"{Fore.GREEN}✓ Successfully authenticated!{Style.RESET_ALL}\n")

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
            disable=self._quiet,
        ) as pbar:
            try:
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

                pbar.set_description("Query execution finished.")
            except Exception as e:
                self._handle_api_error(e)

        self._print(f"{Fore.GREEN}✓ Query executed successfully!{Style.RESET_ALL}\n")
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
