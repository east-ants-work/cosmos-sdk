"""
Product: 상품 정보를 담는 Object

주요 용도: 상품 관리, 재고 관리, 카탈로그
관련 Link: orders (→ Order), category (→ Category)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cosmos_sdk.base import BaseObject, Link, Property

if TYPE_CHECKING:
    from cosmos_sdk.objects.order import Order


class Product(BaseObject):
    """
    Product Object representing a product in the catalog.

    Properties:
        id: Unique product identifier (primary key)
        sku: Stock Keeping Unit
        name: Product name
        description: Product description
        price: Product price
        currency: Price currency
        stock: Current stock quantity
        category: Product category
        status: Product status (active, inactive, discontinued)
        tags: Product tags (array)
        attributes: Product attributes (JSON)
        created_at: When the product was created
        updated_at: When the product was last updated

    Links:
        orders: Orders containing this product (many-to-many)
    """

    __object_type_key__ = "product"
    __primary_key__ = "id"

    # Properties
    id = Property(type="string", primary_key=True)
    sku = Property(type="string", required=True, indexed=True, description="Stock Keeping Unit")
    name = Property(type="string", required=True, indexed=True)
    description = Property(type="string")
    price = Property(type="float", required=True)
    currency = Property(type="string", description="Price currency (KRW, USD, etc.)")
    cost = Property(type="float", description="Cost price")
    stock = Property(type="int", description="Current stock quantity")
    category = Property(type="string", indexed=True)
    brand = Property(type="string", indexed=True)
    status = Property(
        type="string",
        indexed=True,
        description="active | inactive | discontinued",
    )
    tags = Property(type="array", description="Product tags")
    attributes = Property(type="json", description="Product attributes")
    image_url = Property(type="string", description="Main product image URL")
    created_at = Property(type="datetime")
    updated_at = Property(type="datetime")

    # Links
    orders: Link[list[Order]] = Link("Order", many=True, reverse="products")
