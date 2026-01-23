"""
Basic Usage Example - Cosmos SDK

This example demonstrates the fundamental operations of the Cosmos SDK.
"""

import asyncio
from cosmos_sdk import CosmosClient
from cosmos_sdk.objects import Customer, Order, Product


async def main():
    # Initialize client
    # Token can also be set via COSMOS_AUTH_TOKEN environment variable
    client = CosmosClient(
        token="your-jwt-token",
        base_url="http://localhost:8080",
    )

    # Alternative: use context manager for automatic cleanup
    async with CosmosClient(token="your-jwt-token") as client:
        # ================================================
        # Single Object Retrieval
        # ================================================

        # Get a customer by ID
        customer = await client.objects.Customer.get("cust_123")
        print(f"Customer: {customer.name}")
        print(f"Email: {customer.email}")
        print(f"Status: {customer.status}")

        # ================================================
        # List Objects
        # ================================================

        # List all customers (with pagination)
        customers = await client.objects.Customer.list()
        print(f"\nFound {len(customers)} customers")

        for customer in customers:
            print(f"  - {customer.name} ({customer.status})")

        # ================================================
        # Iterate Through All Results
        # ================================================

        # Automatically handles pagination
        print("\nAll customers:")
        async for customer in client.objects.Customer.iterate():
            print(f"  - {customer.name}")

        # ================================================
        # Convert to DataFrame
        # ================================================

        # Get results as Polars DataFrame
        df = await client.objects.Customer.limit(100).list()
        df_polars = df.to_dataframe()
        print(f"\nDataFrame shape: {df_polars.shape}")
        print(df_polars.head())

        # Convert to Pandas if needed
        df_pandas = df_polars.to_pandas()


if __name__ == "__main__":
    asyncio.run(main())
