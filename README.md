# Chakra Python SDK

[![PyPI version](https://badge.fury.io/py/chakra-py.svg)](https://badge.fury.io/py/chakra-py)
[![Build Status](https://github.com/Chakra-Network/python-sdk/actions/workflows/test.yml/badge.svg)](https://github.com/Chakra-Network/python-sdk/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python versions](https://img.shields.io/pypi/pyversions/chakra-py.svg)](https://pypi.org/project/chakra-py/)


![python_sdk](https://github.com/user-attachments/assets/d87bdee1-62b0-4ed8-b8ad-ec63c261a2ab)


A Python SDK for interacting with the Chakra API. This SDK provides seamless integration with pandas DataFrames for data querying and manipulation.

## Features

- **Token-based Authentication**: Secure authentication using DB Session keys
- **Pandas Integration**: Query results automatically converted to pandas DataFrames
- **Automatic Table Management**: Create and update tables with schema inference
- **Batch Operations**: Efficient data pushing with batched inserts

## Installation

```bash
pip install chakra-py
```

## Finding your DB Session Key

1. Login to the [Chakra Console](https://console.chakra.dev/)
2. Select Settings
3. Navigate to the releveant database and copy the DB Session Key (not the access key or secret access key)

https://github.com/user-attachments/assets/9f1c1ab8-cb87-42a1-8627-184617bbb7d7

## Quick Start

```python
from chakra_py import Chakra
import pandas as pd

# Initialize client
client = Chakra("YOUR_DB_SESSION_KEY")

# Query data (returns pandas DataFrame)
df = client.execute("SELECT * FROM my_table")
print(df.head())

# Push data to a new or existing table
data = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "score": [85.5, 92.0, 78.5]
})
client.push("students", data, create_if_missing=True)
```

## Querying Data

Execute SQL queries and receive results as pandas DataFrames:

```python
# Simple query
df = client.execute("SELECT * FROM table_name")

# Complex query with aggregations
df = client.execute("""
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
client.push(
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
client.push("users", new_users, create_if_missing=False)
```

The SDK automatically:
- Infers appropriate column types from DataFrame dtypes
- Creates tables with proper schema when needed
- Handles NULL values and type conversions
- Performs batch inserts for better performance

## Development

To contribute to the SDK:

1. Clone the repository
```bash
git clone https://github.com/Chakra-Network/python-sdk.git
cd python-sdk
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
