# Chakra Network Python SDK

A Python SDK for interacting with the Chakra Network API. This SDK provides a simple interface for querying and pushing data to Chakra databases.

## Installation

```bash
pip install chakra-sdk
```

## Authentication

The SDK supports two authentication methods:

### 1. Credential-based Authentication (Recommended)

Use your Chakra Network access key ID and secret key to authenticate:

```python
from chakra_sdk import ChakraClient

client = ChakraClient(
    api_base_url="http://api.chakra.dev",
    access_key_id="YOUR_ACCESS_KEY_ID",
    secret_key="YOUR_SECRET_KEY",
    username="YOUR_USERNAME"  # Optional, defaults to "default"
)
```

### 2. Token-based Authentication (Legacy)

If you already have a valid token, you can authenticate directly:

```python
from chakra_sdk import ChakraClient

client = ChakraClient(
    api_base_url="http://api.chakra.dev",
    token="YOUR_TOKEN"
)
```

## Usage

### Querying Data

Execute SQL queries and receive results as pandas DataFrames:

```python
# Execute a query
df = client.query("SELECT * FROM example_table")

# Work with the results using pandas
print(df.head())
print(df.describe())
```

### Pushing Data

Push pandas DataFrames or dictionaries to new or existing tables:

```python
import pandas as pd

# Create a sample DataFrame
data = pd.DataFrame({
    'column1': [1, 2, 3],
    'column2': ['a', 'b', 'c']
})

# Push data to a table
client.push_data("my_table", data)
```

## Error Handling

The SDK provides custom exceptions for better error handling:

```python
from chakra_sdk import ChakraAuthError, SessionCreationError

try:
    client = ChakraClient(
        api_base_url="http://api.chakra.dev",
        access_key_id="INVALID_KEY",
        secret_key="INVALID_SECRET"
    )
except SessionCreationError as e:
    print(f"Failed to create session: {e}")
except ChakraAuthError as e:
    print(f"Authentication error: {e}")
```

## Development

### Setup

1. Clone the repository:
```bash
git clone https://github.com/Chakra-Network/python-sdk.git
cd python-sdk
```

2. Install dependencies:
```bash
pip install poetry
poetry install
```

### Running Tests

```bash
poetry run pytest
```

### Code Formatting

```bash
poetry run black .
poetry run isort .
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
