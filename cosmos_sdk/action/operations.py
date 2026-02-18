"""
Action operations.

Provides a clean API for Actions to perform state transitions.

Usage:
    from cosmos_sdk.action import override, create_object, delete_object, SET

    async def execute(target, **params):
        # Clean API - pass target directly
        await override(target, operations=[SET("status", "approved")])
        await delete_object(target)
"""

from __future__ import annotations

from typing import Any

from cosmos_sdk._internal.types import (
    AllowedOperation as AllowedOperationEnum,
    OverrideChange,
    OverrideResult,
    CreateObjectResult,
    ClearOverrideResult,
)
from cosmos_sdk.action.context import get_cosmos_context


# ========================================
# Operation Helpers
# ========================================


def SET(property: str, value: Any) -> OverrideChange:
    """Set a property value directly."""
    return OverrideChange(property=property, op=AllowedOperationEnum.SET, value=value)


def TRANSITION(property: str, value: Any) -> OverrideChange:
    """Perform a state transition (from -> to)."""
    return OverrideChange(property=property, op=AllowedOperationEnum.TRANSITION, value=value)


def INCREMENT(property: str, value: int | float = 1) -> OverrideChange:
    """Increment a numeric property."""
    return OverrideChange(property=property, op=AllowedOperationEnum.INCREMENT, value=value)


def DECREMENT(property: str, value: int | float = 1) -> OverrideChange:
    """Decrement a numeric property."""
    return OverrideChange(property=property, op=AllowedOperationEnum.DECREMENT, value=value)


def APPEND(property: str, value: Any) -> OverrideChange:
    """Append a value to an array property."""
    return OverrideChange(property=property, op=AllowedOperationEnum.APPEND, value=value)


def REMOVE(property: str, value: Any) -> OverrideChange:
    """Remove a value from an array property."""
    return OverrideChange(property=property, op=AllowedOperationEnum.REMOVE, value=value)


# ========================================
# Main Operations
# ========================================


async def override(
    target: dict | str | None = None,
    operations: list[OverrideChange] | None = None,
    *,
    object_type: str | None = None,
    object_id: str | None = None,
    action_id: str | None = None,
    tenant_id: str | None = None,
) -> OverrideResult:
    """
    Apply state changes to an Object.

    Args:
        target: Target object dict (with _object_type, _object_id) - preferred
        operations: List of changes to apply (use SET, TRANSITION, etc.)
        object_type: Type of the object (alternative to target)
        object_id: ID of the object (alternative to target)
        action_id: Optional action ID for audit trail
        tenant_id: Tenant ID (defaults to 'default')

    Returns:
        OverrideResult with applied_count and updated_objects

    Example:
        from cosmos_sdk.action import override, SET, TRANSITION

        # Clean way - pass target directly
        await override(target, operations=[
            TRANSITION("status", "approved"),
            SET("approved_at", datetime.now().isoformat())
        ])

        # Explicit way
        await override(
            object_type="Order",
            object_id="order_123",
            operations=[...]
        )
    """
    # Extract object_type and object_id from target if provided
    if target is not None and isinstance(target, dict):
        object_type = target.get("_object_type") or object_type
        object_id = target.get("_object_id") or object_id

    if not object_type or not object_id:
        raise ValueError("Either target dict or object_type/object_id must be provided")

    if operations is None:
        operations = []

    objectdb = get_cosmos_context()
    return await objectdb.override(
        object_type=object_type,
        object_ids=[object_id],
        changes=operations,
        action_id=action_id,
        tenant_id=tenant_id,
    )


async def create_object(
    object_type: str,
    object_id: str,
    properties: dict[str, Any],
    tenant_id: str | None = None,
) -> CreateObjectResult:
    """
    Create a new Object.

    Args:
        object_type: Type of the object to create
        object_id: Unique ID for the new object
        properties: Initial property values
        tenant_id: Tenant ID (defaults to 'default')

    Returns:
        CreateObjectResult with the created object info

    Example:
        from cosmos_sdk.action import create_object

        result = await create_object(
            object_type="AuditLog",
            object_id="log_001",
            properties={
                "action": "order_approved",
                "target_id": target["_object_id"],
                "timestamp": datetime.now().isoformat()
            }
        )
    """
    objectdb = get_cosmos_context()
    return await objectdb.create_object(
        object_type=object_type,
        object_id=object_id,
        properties=properties,
        tenant_id=tenant_id,
    )


async def delete_object(
    target: dict | None = None,
    *,
    object_type: str | None = None,
    object_id: str | None = None,
    tenant_id: str | None = None,
) -> None:
    """
    Delete an Object.

    Args:
        target: Target object dict (with _object_type, _object_id) - preferred
        object_type: Type of the object to delete (alternative to target)
        object_id: ID of the object to delete (alternative to target)
        tenant_id: Tenant ID (defaults to 'default')

    Example:
        from cosmos_sdk.action import delete_object

        # Clean way - pass target directly
        await delete_object(target)

        # Explicit way
        await delete_object(
            object_type="Order",
            object_id="order_123"
        )
    """
    # Extract object_type and object_id from target if provided
    if target is not None and isinstance(target, dict):
        object_type = target.get("_object_type") or object_type
        object_id = target.get("_object_id") or object_id

    if not object_type or not object_id:
        raise ValueError("Either target dict or object_type/object_id must be provided")

    objectdb = get_cosmos_context()
    await objectdb.delete_object(
        object_type=object_type,
        object_id=object_id,
        tenant_id=tenant_id,
    )


async def clear_override(
    target: dict | None = None,
    properties: list[str] | None = None,
    *,
    object_type: str | None = None,
    object_id: str | None = None,
    tenant_id: str | None = None,
) -> ClearOverrideResult:
    """
    Clear overrides and revert to Fact values.

    Args:
        target: Target object dict (with _object_type, _object_id) - preferred
        properties: List of property names to clear overrides for
        object_type: Type of the object (alternative to target)
        object_id: ID of the object (alternative to target)
        tenant_id: Tenant ID (defaults to 'default')

    Returns:
        ClearOverrideResult with cleared_count and updated_objects

    Example:
        from cosmos_sdk.action import clear_override

        # Clean way - pass target directly
        await clear_override(target, properties=["status", "approved_at"])

        # Explicit way
        await clear_override(
            object_type="Order",
            object_id="order_123",
            properties=["status", "approved_at"]
        )
    """
    # Extract object_type and object_id from target if provided
    if target is not None and isinstance(target, dict):
        object_type = target.get("_object_type") or object_type
        object_id = target.get("_object_id") or object_id

    if not object_type or not object_id:
        raise ValueError("Either target dict or object_type/object_id must be provided")

    if properties is None:
        properties = []

    objectdb = get_cosmos_context()
    return await objectdb.clear_override(
        object_type=object_type,
        object_ids=[object_id],
        properties=properties,
        tenant_id=tenant_id,
    )
