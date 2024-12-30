# Chakra SDK

A Python SDK for interacting with the Chakra Network API (api.chakra.dev). This SDK provides seamless integration with pandas DataFrames for data querying and manipulation.

## Features

- **Token-based Authentication**: Secure authentication using DDB tokens
- **Pandas Integration**: Query results automatically converted to pandas DataFrames
- **Automatic Table Management**: Create and update tables with schema inference
- **Batch Operations**: Efficient data pushing with batched inserts

## Installation

```bash
pip install chakra-sdk
```

## Quick Start

```python
from chakra_sdk import ChakraClient
import pandas as pd

# Initialize client
client = ChakraClient()

# Authenticate (token must start with "DDB_")
client.auth.login("DDB_your_token_here")

# Query data (returns pandas DataFrame)
df = client.query.execute("SELECT * FROM my_table")
print(df.head())

# Push data to a new or existing table
data = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "score": [85.5, 92.0, 78.5]
})
client.data.push("students", data, create_if_missing=True)
```

## Authentication

The SDK uses token-based authentication. Your token must start with "DDB_" prefix:

```python
# Valid token
client.auth.login("DDB_your_token")  # ✓ Works

# Invalid token
client.auth.login("invalid_token")    # ✗ Raises ValueError
```

## Querying Data

Execute SQL queries and receive results as pandas DataFrames:

```python
# Simple query
df = client.query.execute("SELECT * FROM table_name")

# Complex query with aggregations
df = client.query.execute("""
    SELECT 
        category,
        COUNT(*) as count,
        AVG(value) as avg_value
    FROM measurements
    GROUP BY category
    HAVING count > 10
    ORDER BY avg_value DESC
""")

# Work with results using pandas
print(df.describe())
print(df.groupby('category').agg({'value': ['mean', 'std']}))
```

## Pushing Data

Push data from pandas DataFrames to tables with automatic schema handling:

```python
# Create a sample DataFrame
df = pd.DataFrame({
    'id': range(1, 1001),
    'name': [f'User_{i}' for i in range(1, 1001)],
    'score': np.random.normal(75, 15, 1000).round(2),
    'active': np.random.choice([True, False], 1000)
})

# Create new table with inferred schema
client.data.push(
    table_name="users",
    data=df,
    create_if_missing=True  # Creates table if it doesn't exist
)

# Update existing table
new_users = pd.DataFrame({
    'id': range(1001, 1101),
    'name': [f'User_{i}' for i in range(1001, 1101)],
    'score': np.random.normal(75, 15, 100).round(2),
    'active': np.random.choice([True, False], 100)
})
client.data.push("users", new_users, create_if_missing=False)
```

The SDK automatically:
- Infers appropriate column types from DataFrame dtypes
- Creates tables with proper schema when needed
- Handles NULL values and type conversions
- Performs batch inserts for better performance

## Error Handling

The SDK provides clear error messages for common scenarios:

```python
try:
    # Authentication errors
    client.query.execute("SELECT * FROM table")  # Without login
except ValueError as e:
    print("Auth error:", e)  # "Authentication required"

try:
    client.auth.login("invalid")  # Wrong token format
except ValueError as e:
    print("Token error:", e)  # "Token must start with 'DDB_'"

try:
    # API errors
    df = client.query.execute("SELECT * FROM nonexistent_table")
except requests.exceptions.HTTPError as e:
    print("API error:", e)
```

## Development

To contribute to the SDK:

1. Clone the repository
```bash
git clone https://github.com/Chakra-Network/chakra-sdk.git
cd chakra-sdk
```

2. Install development dependencies with Poetry
```bash
# Install Poetry if you haven't already
curl -sSL https://install.python-poetry.org | python3 -

# Install dependencies
poetry install
```

3. Run tests
```bash
poetry run pytest
```

4. Build package
```bash
poetry build
```

## PyPI Publication

The package is configured for easy PyPI publication:

1. Update version in `pyproject.toml`
2. Build distribution: `poetry build`
3. Publish: `poetry publish`

## License

MIT License - see LICENSE file for details.

## Support

For support and questions, please open an issue in the GitHub repository.
