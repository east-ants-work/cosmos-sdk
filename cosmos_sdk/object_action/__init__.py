"""
Object Action module for Cosmos SDK.

Provides a clean API for Object Actions to perform state transitions.

Usage:
    from cosmos_sdk.object_action import override, create_object, delete_object

    # In Object Action execute function:
    async def execute(target, **params):
        await override(
            object_type=target["_object_type"],
            object_id=target["_object_id"],
            operations=[SET("status", "approved")]
        )
"""

from cosmos_sdk.object_action.context import (
    set_cosmos_context,
    get_cosmos_context,
)
from cosmos_sdk.object_action.operations import (
    override,
    create_object,
    delete_object,
    clear_override,
    SET,
    TRANSITION,
    INCREMENT,
    DECREMENT,
    APPEND,
    REMOVE,
)

__all__ = [
    # Context management (for internal use)
    "set_cosmos_context",
    "get_cosmos_context",
    # Operations
    "override",
    "create_object",
    "delete_object",
    "clear_override",
    # Operation helpers
    "SET",
    "TRANSITION",
    "INCREMENT",
    "DECREMENT",
    "APPEND",
    "REMOVE",
]
