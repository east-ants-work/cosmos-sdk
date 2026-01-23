"""
Aggregations Example - Cosmos SDK

This example demonstrates how to perform aggregations and grouping.
"""

import asyncio
from cosmos_sdk import CosmosClient, Agg
from cosmos_sdk.objects import Order, Customer


async def main():
    async with CosmosClient() as client:
        # ================================================
        # Simple Aggregations
        # ================================================

        # Count
        total_orders = await client.objects.Order.count()
        print(f"Total orders: {total_orders}")

        # Count with filter
        completed_count = await client.objects.Order.where(
            Order.status == "completed"
        ).count()
        print(f"Completed orders: {completed_count}")

        # Sum
        total_revenue = await client.objects.Order.where(
            Order.status == "completed"
        ).sum(Order.total)
        print(f"Total revenue: ${total_revenue:,.2f}")

        # Average
        avg_order_value = await client.objects.Order.where(
            Order.status == "completed"
        ).avg(Order.total)
        print(f"Average order value: ${avg_order_value:,.2f}")

        # Min/Max
        min_order = await client.objects.Order.min(Order.total)
        max_order = await client.objects.Order.max(Order.total)
        print(f"Order value range: ${min_order} - ${max_order}")

        # ================================================
        # Group By Aggregations
        # ================================================

        # Group by status
        orders_by_status = await client.objects.Order.group_by(
            Order.status
        ).agg(
            count=Agg.count(),
            total_value=Agg.sum(Order.total),
            avg_value=Agg.avg(Order.total),
        ).list()

        print("\nOrders by status:")
        for row in orders_by_status:
            print(f"  {row['status']}: {row['count']} orders, ${row['total_value']:,.2f} total")

        # Group by multiple fields
        monthly_stats = await client.objects.Order.group_by(
            "status", "currency"
        ).agg(
            count=Agg.count(),
            total=Agg.sum(Order.total),
        ).list()

        print("\nMonthly stats by status and currency:")
        for row in monthly_stats:
            print(f"  {row['status']}/{row['currency']}: {row['count']} orders")

        # ================================================
        # Group By with DataFrame Output
        # ================================================

        # Get results directly as DataFrame
        df = client.objects.Order.where(
            Order.status == "completed"
        ).group_by(Order.status).agg(
            count=Agg.count(),
            total=Agg.sum(Order.total),
            min_value=Agg.min(Order.total),
            max_value=Agg.max(Order.total),
        ).to_dataframe()

        print("\nAggregation as DataFrame:")
        print(df)

        # ================================================
        # Customer Analytics Example
        # ================================================

        # Top customers by order count
        customer_stats = await client.objects.Order.group_by(
            "customer_id"  # Assuming there's a customer_id field
        ).agg(
            order_count=Agg.count(),
            total_spent=Agg.sum(Order.total),
            avg_order=Agg.avg(Order.total),
        ).list()

        # Sort by total spent (in Python since we have all data)
        top_customers = sorted(
            customer_stats,
            key=lambda x: x.get("total_spent", 0),
            reverse=True
        )[:10]

        print("\nTop 10 customers by spending:")
        for i, customer in enumerate(top_customers, 1):
            print(
                f"  {i}. Customer {customer['customer_id']}: "
                f"{customer['order_count']} orders, ${customer['total_spent']:,.2f}"
            )


if __name__ == "__main__":
    asyncio.run(main())
