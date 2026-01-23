"""
Order: 주문 정보를 담는 Object

주요 용도: 주문 관리, 매출 분석, 배송 추적
관련 Link: customer (→ Customer), products (→ Product)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cosmos_sdk.base import BaseObject, Link, Property

if TYPE_CHECKING:
    from cosmos_sdk.objects.customer import Customer
    from cosmos_sdk.objects.product import Product


class Order(BaseObject):
    """
    Order Object representing an order in the system.

    Properties:
        id: Unique order identifier (primary key)
        order_number: Human-readable order number
        status: Order status (pending, confirmed, shipped, delivered, cancelled)
        total: Order total amount
        currency: Currency code (e.g., KRW, USD)
        shipping_address: Shipping address (JSON)
        notes: Order notes
        ordered_at: When the order was placed
        shipped_at: When the order was shipped
        delivered_at: When the order was delivered

    Links:
        customer: Customer who placed the order (many-to-one)
        products: Products in this order (many-to-many)
    """

    __object_type_key__ = "order"
    __primary_key__ = "id"

    # Properties
    id = Property(type="string", primary_key=True)
    order_number = Property(type="string", required=True, indexed=True)
    status = Property(
        type="string",
        indexed=True,
        description="pending | confirmed | shipped | delivered | cancelled",
    )
    total = Property(type="float", description="Order total amount")
    currency = Property(type="string", description="Currency code (KRW, USD, etc.)")
    quantity = Property(type="int", description="Total quantity of items")
    shipping_address = Property(type="json", description="Shipping address details")
    notes = Property(type="string", description="Order notes")
    ordered_at = Property(type="datetime")
    shipped_at = Property(type="datetime")
    delivered_at = Property(type="datetime")
    created_at = Property(type="datetime")
    updated_at = Property(type="datetime")

    # Links
    customer: Link[Customer] = Link("Customer", many=False, reverse="orders")
    products: Link[list[Product]] = Link("Product", many=True, reverse="orders")
