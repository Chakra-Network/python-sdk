from typing import Any, Dict, Union

import pandas as pd


def _map_pandas_to_duckdb_type(dtype) -> str:
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


class Data:
    """Handles data push operations."""

    def __init__(self, client):
        self._client = client

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
        if not self._client.token:
            raise ValueError("Authentication required. Call client.auth.login() first.")

        if isinstance(data, pd.DataFrame):
            # Convert DataFrame to list of records
            records = data.to_dict(orient="records")

            if create_if_missing:
                # Create table with proper schema
                columns = [
                    {"name": col, "type": _map_pandas_to_duckdb_type(dtype)}
                    for col, dtype in data.dtypes.items()
                ]
                create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
                create_sql += ", ".join(
                    [f"{col['name']} {col['type']}" for col in columns]
                )
                create_sql += ")"

                try:
                    response = self._client._session.post(
                        f"{self._client.base_url}/api/v1/execute",
                        json={"sql": create_sql},
                    )
                    response.raise_for_status()
                except Exception as e:
                    self._client._handle_api_error(e)

            # Insert data using batch execute
            if records:
                placeholders = ", ".join(["?" for _ in records[0]])
                insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

                statements = []
                for record in records:
                    values = [str(v) if pd.notna(v) else None for v in record.values()]
                    stmt = insert_sql.replace("?", "%s") % tuple(
                        (
                            f"'{v}'"
                            if isinstance(v, str)
                            else str(v) if v is not None else "NULL"
                        )
                        for v in values
                    )
                    statements.append(stmt)

                response = self._client._session.post(
                    f"{self._client.base_url}/api/v1/execute/batch",
                    json={"statements": statements},
                )
                response.raise_for_status()
        else:
            # Handle dictionary input
            raise NotImplementedError("Dictionary input not yet implemented")