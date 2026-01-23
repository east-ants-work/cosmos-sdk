"""
Legacy compatibility layer for Cosmos SDK.

Provides backward-compatible functions that map to the new SDK API.
These functions are deprecated and will be removed in a future version.

Usage:
    from cosmos_sdk.compat import getObject, assembleByPath

    # These work like the old API
    df = getObject("Customer", filters={"status": "active"})
    df = assembleByPath("Order", ["order_id"], [{"edge": "has_customer", "select": ["name"]}])
"""

from __future__ import annotations

import asyncio
import warnings
from typing import Any

import polars as pl

from cosmos_sdk.client import CosmosClient

# Global client instance (lazy initialized)
_client: CosmosClient | None = None


def _get_client() -> CosmosClient:
    """Get or create the global client instance."""
    global _client
    if _client is None:
        _client = CosmosClient()
    return _client


def set_client(client: CosmosClient) -> None:
    """
    Set the global client instance.

    Call this before using getObject or assembleByPath if you need
    custom client configuration.

    Args:
        client: CosmosClient instance to use
    """
    global _client
    _client = client


def getObject(
    object_type: str,
    filters: dict[str, Any] | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> pl.DataFrame:
    """
    Get objects of a given type with optional filters.

    DEPRECATED: Use client.objects.<Type>.where(...).to_dataframe() instead.

    Args:
        object_type: Name of the object type (e.g., "Customer")
        filters: Dictionary of field -> value filters (equality only)
        limit: Maximum number of results
        offset: Number of results to skip

    Returns:
        Polars DataFrame with the results
    """
    warnings.warn(
        "getObject() is deprecated. Use client.objects.<Type>.where(...).to_dataframe() instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    client = _get_client()

    async def _fetch() -> pl.DataFrame:
        # Get the object set
        objects_accessor = client.objects
        obj_set = getattr(objects_accessor, object_type)

        # Apply filters
        if filters:
            for field, value in filters.items():
                # Create equality filter
                obj_set = obj_set.where(_create_filter(field, value))

        # Apply pagination
        if limit:
            obj_set = obj_set.limit(limit)
        if offset:
            obj_set = obj_set.offset(offset)

        # Execute and convert
        result = await obj_set.list()
        return result.to_dataframe()

    return asyncio.get_event_loop().run_until_complete(_fetch())


def _create_filter(field: str, value: Any) -> Any:
    """Create a filter condition for a field."""
    from cosmos_sdk.base import PropertyComparison

    return PropertyComparison(field, "eq", value)


def assembleByPath(
    start_object: str,
    start_select: list[str],
    steps: list[dict[str, Any]],
    filters: dict[str, Any] | None = None,
    limit: int | None = None,
) -> pl.DataFrame:
    """
    Assemble objects by traversing a path of edges.

    DEPRECATED: Use client.objects.<Type>.search_around(...).to_dataframe() instead.

    Args:
        start_object: Starting object type name
        start_select: Fields to select from starting object
        steps: List of traversal steps, each with:
            - edge: Link name to traverse
            - select: Fields to select from target
            - filters: Optional filters for target (dict)
        filters: Filters for starting objects
        limit: Maximum number of results

    Returns:
        Polars DataFrame with flattened results
    """
    warnings.warn(
        "assembleByPath() is deprecated. Use client.objects.<Type>.search_around(...).to_dataframe() instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    client = _get_client()

    async def _fetch() -> pl.DataFrame:
        # Get starting object set
        objects_accessor = client.objects
        obj_set = getattr(objects_accessor, start_object)

        # Apply starting filters
        if filters:
            for field, value in filters.items():
                obj_set = obj_set.where(_create_filter(field, value))

        # Apply limit
        if limit:
            obj_set = obj_set.limit(limit)

        # Get starting objects
        start_objects = await obj_set.list()

        if not start_objects:
            return pl.DataFrame()

        # Build result rows
        rows: list[dict[str, Any]] = []

        for start_obj in start_objects:
            row: dict[str, Any] = {}

            # Add selected fields from start object
            for field in start_select:
                row[f"{start_object}.{field}"] = getattr(start_obj, field, None)

            # Traverse each step
            current_objects = [start_obj]

            for step in steps:
                edge = step.get("edge")
                select_fields = step.get("select", [])
                step_filters = step.get("filters", {})

                next_objects = []

                for obj in current_objects:
                    # Load the link
                    try:
                        linked = await obj.load_link(edge)
                        if linked is None:
                            continue
                        if not isinstance(linked, list):
                            linked = [linked]

                        # Apply step filters
                        for linked_obj in linked:
                            if step_filters:
                                match = all(
                                    getattr(linked_obj, f, None) == v
                                    for f, v in step_filters.items()
                                )
                                if not match:
                                    continue

                            # Add selected fields
                            target_type = linked_obj.__class__.__name__
                            for field in select_fields:
                                row[f"{target_type}.{field}"] = getattr(linked_obj, field, None)

                            next_objects.append(linked_obj)

                    except Exception:
                        continue

                current_objects = next_objects

            if row:
                rows.append(row)

        return pl.DataFrame(rows)

    return asyncio.get_event_loop().run_until_complete(_fetch())


# Convenience function to convert new SDK results to old format
def to_legacy_format(df: pl.DataFrame, object_type: str) -> pl.DataFrame:
    """
    Convert a DataFrame to the legacy format with prefixed column names.

    Args:
        df: DataFrame from new SDK
        object_type: Object type name for prefix

    Returns:
        DataFrame with columns prefixed by object type
    """
    return df.rename({col: f"{object_type}.{col}" for col in df.columns})
