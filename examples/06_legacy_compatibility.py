"""
Legacy Compatibility Example - Cosmos SDK

This example shows how to migrate from the old API to the new SDK.
The compat module provides backward-compatible functions.
"""

import asyncio

# ================================================
# OLD WAY (Deprecated)
# ================================================

# These functions work like the old API but are deprecated
from cosmos_sdk.compat import getObject, assembleByPath, set_client
from cosmos_sdk import CosmosClient


def old_way_example():
    """Example using deprecated functions."""

    # Set up client (optional, uses defaults if not called)
    client = CosmosClient(token="your-token")
    set_client(client)

    # Old way: getObject
    df = getObject("Customer", filters={"status": "active"})
    print("Old way - getObject:")
    print(df.head())

    # Old way: assembleByPath
    df = assembleByPath(
        start_object="Order",
        start_select=["order_id", "total"],
        steps=[
            {"edge": "has_customer", "select": ["name", "email"]},
        ],
        filters={"status": "completed"},
        limit=100,
    )
    print("\nOld way - assembleByPath:")
    print(df.head())


# ================================================
# NEW WAY (Recommended)
# ================================================

async def new_way_example():
    """Example using the new SDK API."""

    async with CosmosClient(token="your-token") as client:
        from cosmos_sdk.objects import Customer, Order

        # New way: equivalent to getObject
        customers = await client.objects.Customer.where(
            Customer.status == "active"
        ).list()
        df = customers.to_dataframe()
        print("New way - equivalent to getObject:")
        print(df.head())

        # New way: equivalent to assembleByPath
        df = await client.objects.Order.where(
            Order.status == "completed"
        ).select(
            Order.order_id, Order.total
        ).search_around("customer").select(
            Customer.name, Customer.email
        ).limit(100).to_dataframe()

        print("\nNew way - equivalent to assembleByPath:")
        print(df.head())


# ================================================
# MIGRATION GUIDE
# ================================================

"""
Migration Guide: Old API -> New SDK

1. getObject(type, filters={...})
   ->
   client.objects.<Type>.where(<Type>.field == value).list()

   Example:
   OLD: getObject("Customer", filters={"status": "active", "tier": "gold"})
   NEW: client.objects.Customer.where(
            Customer.status == "active",
            Customer.tier == "gold"
        ).list()

2. assembleByPath(start, select, steps)
   ->
   client.objects.<Type>.select(...).search_around(link).select(...).list()

   Example:
   OLD: assembleByPath(
            "Order",
            ["order_id"],
            [{"edge": "has_customer", "select": ["name"]}]
        )
   NEW: client.objects.Order.select(
            Order.order_id
        ).search_around("customer").select(
            Customer.name
        ).list()

3. Result format:
   OLD: Returns DataFrame directly with prefixed columns (e.g., "Customer.name")
   NEW: Returns ObjectList, call .to_dataframe() for DataFrame

4. Async:
   OLD: Synchronous functions
   NEW: Async functions (use await or asyncio.run())

   For synchronous contexts, the SDK provides sync wrappers:
   - ObjectSet.to_dataframe() runs synchronously
   - GroupedObjectSet.to_dataframe() runs synchronously
"""


if __name__ == "__main__":
    print("=" * 60)
    print("DEPRECATED (Old Way)")
    print("=" * 60)
    # old_way_example()  # Uncomment to test

    print("\n" + "=" * 60)
    print("RECOMMENDED (New Way)")
    print("=" * 60)
    asyncio.run(new_way_example())
