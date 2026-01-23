"""
Cosmos SDK - Python SDK for Cosmos Object Graph

ORM-style API for interacting with Objects and Links.

Example:
    from cosmos_sdk import CosmosClient
    from cosmos_sdk.objects import Customer, Order

    client = CosmosClient(token="eyJhbG...")

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

Legacy compatibility (deprecated):
    from cosmos_sdk.compat import getObject, assembleByPath

    df = getObject("Customer", filters={"status": "active"})
"""

from cosmos_sdk.client import CosmosClient, create_client
from cosmos_sdk.base import (
    Agg,
    BaseObject,
    GroupedObjectSet,
    Link,
    ObjectList,
    ObjectSet,
    Property,
    PropertyComparison,
)

__version__ = "0.1.0"
__all__ = [
    # Client
    "CosmosClient",
    "create_client",
    # Base classes
    "BaseObject",
    "Property",
    "Link",
    "ObjectSet",
    "ObjectList",
    # Aggregation
    "Agg",
    "GroupedObjectSet",
    # Filtering
    "PropertyComparison",
]
