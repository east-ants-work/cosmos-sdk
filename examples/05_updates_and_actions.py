"""
Updates and Actions Example - Cosmos SDK

This example demonstrates how to update objects using the Actions API.
"""

import asyncio
from cosmos_sdk import CosmosClient
from cosmos_sdk.objects import Order, Customer


async def main():
    async with CosmosClient() as client:
        # ================================================
        # Update Single Object
        # ================================================

        # Get an order
        order = await client.objects.Order.get("order_123")
        print(f"Current status: {order.status}")

        # Update single field
        await order.update(status="shipped")
        print(f"Updated status: {order.status}")

        # Update multiple fields
        await order.update(
            status="delivered",
            delivered_at="2024-01-15T14:30:00Z",
        )
        print(f"Final status: {order.status}")

        # ================================================
        # Refresh Object Data
        # ================================================

        # Reload from server to get latest state
        await order.refresh()
        print(f"Refreshed status: {order.status}")

        # ================================================
        # Update Customer Example
        # ================================================

        customer = await client.objects.Customer.get("cust_123")

        # Update tier based on spending
        if customer.total_spent > 10000:
            await customer.update(tier="gold")
        elif customer.total_spent > 5000:
            await customer.update(tier="silver")

        print(f"Customer {customer.name} tier: {customer.tier}")

        # ================================================
        # Bulk Updates (via Actions API)
        # ================================================

        # Note: For bulk updates, you'd typically use the Actions API directly
        from cosmos_sdk._internal.api import ObjectDBClient
        from cosmos_sdk._internal.types import (
            ApplyActionInput,
            PropertyChange,
            AllowedOperation,
        )

        # Get the underlying API client
        api_client = client._api_client

        # Apply action to multiple objects
        action = ApplyActionInput(
            object_type="order",
            object_ids=["order_1", "order_2", "order_3"],
            changes=[
                PropertyChange(
                    property="status",
                    op=AllowedOperation.SET,
                    value="cancelled",
                )
            ],
        )

        result = await api_client.apply_action(action)
        print(f"Bulk update applied: {result.event_id}")

        # ================================================
        # State Machine Transitions
        # ================================================

        # If the property has a state machine defined,
        # use TRANSITION operation
        order = await client.objects.Order.get("order_456")

        # This would follow the state machine rules
        # (e.g., pending -> confirmed -> shipped -> delivered)
        action = ApplyActionInput(
            object_type="order",
            object_ids=[order.object_id],
            changes=[
                PropertyChange(
                    property="status",
                    op=AllowedOperation.TRANSITION,
                    value="shipped",  # Target state
                )
            ],
        )

        await api_client.apply_action(action)


if __name__ == "__main__":
    asyncio.run(main())
