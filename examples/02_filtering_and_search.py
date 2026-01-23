"""
Filtering and Search Example - Cosmos SDK

This example demonstrates how to filter and search objects.
"""

import asyncio
from cosmos_sdk import CosmosClient
from cosmos_sdk.objects import Customer, Order, Product


async def main():
    async with CosmosClient() as client:
        # ================================================
        # Basic Filtering (Operator Overloading)
        # ================================================

        # Equality
        active_customers = await client.objects.Customer.where(
            Customer.status == "active"
        ).list()
        print(f"Active customers: {len(active_customers)}")

        # Comparison operators
        recent_orders = await client.objects.Order.where(
            Order.ordered_at >= "2024-01-01",
            Order.total > 100.0,
        ).list()
        print(f"Recent high-value orders: {len(recent_orders)}")

        # ================================================
        # Complex Conditions (AND/OR)
        # ================================================

        # OR condition using |
        vip_or_active = await client.objects.Customer.where(
            (Customer.status == "vip") | (Customer.status == "active")
        ).list()
        print(f"VIP or Active customers: {len(vip_or_active)}")

        # NOT condition using ~
        non_cancelled = await client.objects.Order.where(
            ~(Order.status == "cancelled")
        ).list()
        print(f"Non-cancelled orders: {len(non_cancelled)}")

        # Combined conditions
        premium_active = await client.objects.Customer.where(
            (Customer.status == "active") & (Customer.tier == "gold"),
            ~(Customer.email.is_null())
        ).list()
        print(f"Premium active customers: {len(premium_active)}")

        # ================================================
        # Special Filters
        # ================================================

        # NULL checks
        customers_with_email = await client.objects.Customer.where(
            Customer.email.is_not_null()
        ).list()

        # IN operator
        specific_statuses = await client.objects.Order.where(
            Order.status.is_in(["pending", "confirmed", "shipped"])
        ).list()

        # LIKE pattern matching
        gmail_users = await client.objects.Customer.where(
            Customer.email.like("%@gmail.com")
        ).list()

        # ================================================
        # Text Search
        # ================================================

        # Search across indexed text fields
        search_results = await client.objects.Customer.search("홍길동").list()
        print(f"Search results for '홍길동': {len(search_results)}")

        # Combine search with filters
        active_hong = await client.objects.Customer.search("홍길동").where(
            Customer.status == "active"
        ).list()
        print(f"Active customers named '홍길동': {len(active_hong)}")

        # ================================================
        # Sorting and Pagination
        # ================================================

        # Sort by field
        top_customers = await client.objects.Customer.order_by(
            Customer.total_spent, order="desc"
        ).limit(10).list()

        # Pagination
        page_2 = await client.objects.Customer.order_by(
            Customer.created_at
        ).limit(20).offset(20).list()


if __name__ == "__main__":
    asyncio.run(main())
