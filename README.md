# Chakra Network Python SDK

A Python SDK for interacting with the Chakra Network API. This SDK provides a simple interface for querying and pushing data to Chakra databases.

## Installation

```bash
pip install chakra-sdk
```

## Authentication

Authenticate with your Chakra Network credentials using your access key ID and secret key:

```python
from chakra_sdk import ChakraClient

client = ChakraClient(
    api_base_url="http://api.chakra.dev",
    access_key_id="YOUR_ACCESS_KEY_ID",
    secret_key="YOUR_SECRET_KEY",
    username="YOUR_USERNAME"  # Optional, defaults to "default"
)
```

The SDK will automatically handle session creation and token management internally.

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

The SDK provides custom exceptions for handling authentication and session-related errors:

```python
from chakra_sdk import ChakraAuthError, SessionCreationError, SessionValidationError

try:
    # Initialize client with credentials
    client = ChakraClient(
        api_base_url="http://api.chakra.dev",
        access_key_id="YOUR_ACCESS_KEY_ID",
        secret_key="YOUR_SECRET_KEY",
        username="YOUR_USERNAME"  # Optional
    )
    
    # Execute operations
    df = client.query("SELECT * FROM example_table")
    
except SessionCreationError as e:
    print(f"Failed to create session: {e}")
    # Handle invalid credentials or connection issues
    
except SessionValidationError as e:
    print(f"Session validation failed: {e}")
    # Handle session expiration or invalidation
    
except ChakraAuthError as e:
    print(f"Authentication error: {e}")
    # Handle other authentication-related errors
```

The SDK automatically handles session creation and token management internally. If a session becomes invalid, appropriate exceptions will be raised when attempting operations.

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
