"""
Context management for Object Actions.

Uses Python's contextvars for async-safe context storage.
The cosmos_sdk instance is set by the execution runtime before
running an Object Action, making it available to operations.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cosmos_sdk.client import ObjectDBAccessor

# Context variable to hold the cosmos_sdk ObjectDB accessor
_objectdb_context: ContextVar[ObjectDBAccessor | None] = ContextVar(
    "cosmos_objectdb", default=None
)


def set_cosmos_context(objectdb: ObjectDBAccessor) -> None:
    """
    Set the ObjectDB accessor for the current async context.

    Called by the Object Action execution runtime before running execute().
    """
    _objectdb_context.set(objectdb)


def get_cosmos_context() -> ObjectDBAccessor:
    """
    Get the ObjectDB accessor from the current async context.

    Raises:
        RuntimeError: If called outside of an Object Action context.
    """
    objectdb = _objectdb_context.get()
    if objectdb is None:
        raise RuntimeError(
            "cosmos_sdk context not set. "
            "This function must be called within an Object Action execute() function."
        )
    return objectdb


def clear_cosmos_context() -> None:
    """Clear the cosmos_sdk context."""
    _objectdb_context.set(None)
