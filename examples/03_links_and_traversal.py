"""
Links and Graph Traversal Example - Cosmos SDK

This example demonstrates how to work with object relationships (Links).
"""

import asyncio
from cosmos_sdk import CosmosClient
from cosmos_sdk.objects import Customer, Order, Product


async def main():
    async with CosmosClient() as client:
        # ================================================
        # Lazy Loading (Single Object)
        # ================================================

        # Get an order
        order = await client.objects.Order.get("order_123")

        # Access linked customer (lazy loads on first access)
        # Note: In async context, use load_link() instead
        customer = await order.load_link("customer")
        print(f"Order {order.order_number} belongs to {customer.name}")

        # Access linked products (one-to-many)
        products = await order.load_link("products")
        print(f"Order contains {len(products)} products:")
        for product in products:
            print(f"  - {product.name}: ${product.price}")

        # ================================================
        # Eager Loading (Avoid N+1)
        # ================================================

        # BAD: N+1 problem (don't do this!)
        # orders = await client.objects.Order.list()
        # for order in orders:
        #     customer = await order.load_link("customer")  # API call per order!

        # GOOD: Eager loading with include
        orders = await client.objects.Order.include("customer").limit(100).list()
        for order in orders:
            # Customer is already loaded, no additional API call
            customer = await order.load_link("customer")
            print(f"Order {order.order_number}: {customer.name}")

        # Load multiple links
        orders = await client.objects.Order.include(
            "customer", "products"
        ).list()

        # ================================================
        # Graph Traversal (search_around)
        # ================================================

        # Single hop: Orders -> Customers
        # "Find all customers who have completed orders"
        customers = await client.objects.Order.where(
            Order.status == "completed"
        ).search_around("customer").list()
        print(f"\nCustomers with completed orders: {len(customers)}")

        # Multi-hop chaining: Orders -> Customers -> (via reverse link)
        # "Find all orders from VIP customers"
        vip_orders = await client.objects.Customer.where(
            Customer.tier == "vip"
        ).search_around("orders").list()
        print(f"Orders from VIP customers: {len(vip_orders)}")

        # Complex traversal with filters
        # "Find addresses in Seoul for completed order customers"
        seoul_addresses = await client.objects.Order.where(
            Order.status == "completed"
        ).search_around("customer").search_around("address").where(
            # Note: Address filters would apply here
        ).list()

        # ================================================
        # Select Specific Fields
        # ================================================

        # Select only needed fields for efficiency
        df = await client.objects.Order.select(
            Order.order_number, Order.total
        ).search_around("customer").select(
            Customer.name, Customer.email
        ).to_dataframe()

        print("\nOrder summary:")
        print(df)

        # ================================================
        # Get All Edges
        # ================================================

        # See all relationships for an object
        customer = await client.objects.Customer.get("cust_123")
        object_links = await customer.links()

        print(f"\nLinks for customer {customer.name}:")
        for link in object_links:
            print(f"  - {link.link_type}: -> {link.target_id}")


if __name__ == "__main__":
    asyncio.run(main())
