# Cosmos SDK Examples

This directory contains examples demonstrating the usage of the Cosmos SDK.

## Examples

1. **01_basic_usage.py** - Basic operations: client initialization, single object retrieval, listing, iteration, DataFrame conversion

2. **02_filtering_and_search.py** - Filtering with operator overloading, complex conditions (AND/OR/NOT), text search, sorting, pagination

3. **03_links_and_traversal.py** - Working with relationships: lazy loading, eager loading, graph traversal with search_around()

4. **04_aggregations.py** - Aggregations: count, sum, avg, min, max, group_by with Agg helpers

5. **05_updates_and_actions.py** - Updating objects, refresh, bulk updates via Actions API

6. **06_legacy_compatibility.py** - Migration guide from old getObject/assembleByPath API

## Running Examples

```bash
# Install the SDK
pip install -e .

# Set environment variables (or pass to CosmosClient)
export COSMOS_AUTH_TOKEN="your-jwt-token"
export COSMOS_API_URL="http://localhost:8080"

# Run an example
python examples/01_basic_usage.py
```

## Quick Start

```python
import asyncio
from cosmos_sdk import CosmosClient
from cosmos_sdk.objects import Customer, Order

async def main():
    async with CosmosClient() as client:
        # Query with filters
        customers = await client.objects.Customer.where(
            Customer.status == "active"
        ).list()

        # Convert to DataFrame
        df = customers.to_dataframe()
        print(df)

asyncio.run(main())
```
