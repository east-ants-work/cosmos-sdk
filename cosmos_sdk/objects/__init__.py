"""
Generated Object classes for Cosmos SDK.

This module exports all Object types available in the current Graph.
AI Agents can use this to discover available Objects:

    from cosmos_sdk.objects import __all__
    print(__all__)  # ["Customer", "Order", "Product"]
"""

# Sample objects (will be auto-generated per Graph)
from cosmos_sdk.objects.customer import Customer
from cosmos_sdk.objects.order import Order
from cosmos_sdk.objects.product import Product

__all__ = ["Customer", "Order", "Product"]
