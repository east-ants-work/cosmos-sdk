"""
Customer: 고객 정보를 담는 Object

주요 용도: 주문, 결제, 마케팅 분석
관련 Link: orders (→ Order), address (→ Address)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cosmos_sdk.base import BaseObject, Link, Property

if TYPE_CHECKING:
    from cosmos_sdk.objects.order import Order


class Customer(BaseObject):
    """
    Customer Object representing a customer in the system.

    Properties:
        id: Unique customer identifier (primary key)
        name: Customer's full name
        email: Customer's email address
        phone: Customer's phone number
        status: Customer status (active, inactive, vip)
        created_at: When the customer was created
        updated_at: When the customer was last updated

    Links:
        orders: Orders placed by this customer (one-to-many)
    """

    __object_type_key__ = "customer"
    __primary_key__ = "id"

    # Properties
    id = Property(type="string", primary_key=True)
    name = Property(type="string", required=True, indexed=True)
    email = Property(type="string", required=True, indexed=True)
    phone = Property(type="string")
    status = Property(type="string", indexed=True, description="active | inactive | vip")
    tier = Property(type="string", description="Customer tier: bronze | silver | gold | platinum")
    total_orders = Property(type="int", description="Total number of orders")
    total_spent = Property(type="float", description="Total amount spent")
    created_at = Property(type="datetime")
    updated_at = Property(type="datetime")

    # Links
    orders: Link[Order] = Link("Order", many=True, reverse="customer")
