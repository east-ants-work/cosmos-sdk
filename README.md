# Cosmos SDK

Python SDK for Cosmos Object Graph - ORM-style API for interacting with Objects and Links.

## Installation

```bash
pip install cosmos-sdk
```

## Usage

```python
from cosmos_sdk import CosmosClient
from cosmos_sdk.objects import Customer, Order

client = CosmosClient()

# Query with filters
customers = await client.objects.Customer.where(
    Customer.status == "active"
).list()

# Convert to DataFrame
df = customers.to_dataframe()

# Traverse links
orders = await client.objects.Order.where(
    Order.status == "completed"
).search_around("customer").list()
```

## Features

- ORM-style API for Objects and Links
- Type-safe property filters
- Relationship traversal with `search_around()`
- Aggregation with `group_by()` and `Agg`
- Polars DataFrame integration
