"""
Cosmos SDK Client.

Main entry point for interacting with the Cosmos Object Graph.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, TypeVar

from cosmos_sdk._internal.api import ObjectDBClient
from cosmos_sdk.base import BaseObject, ObjectSet

if TYPE_CHECKING:
    pass

T = TypeVar("T", bound=BaseObject)


class ObjectsAccessor:
    """
    Accessor for Object types.

    Provides attribute-style access to Object types:
        client.objects.Customer
        client.objects.Order
    """

    def __init__(self, client: CosmosClient, object_registry: dict[str, type[BaseObject]]):
        self._client = client
        self._registry = object_registry

    def __getattr__(self, name: str) -> ObjectSet:
        """Get ObjectSet for the named type."""
        if name.startswith("_"):
            raise AttributeError(name)

        if name not in self._registry:
            # Try to find by case-insensitive match
            for key in self._registry:
                if key.lower() == name.lower():
                    name = key
                    break
            else:
                raise AttributeError(
                    f"Unknown Object type: {name}. "
                    f"Available types: {list(self._registry.keys())}"
                )

        object_type = self._registry[name]
        return ObjectSet(
            self._client,
            object_type,
            object_type.__object_type_key__ or name.lower(),
        )

    def __dir__(self) -> list[str]:
        """List available Object types."""
        return list(self._registry.keys())

    def register(self, object_type: type[BaseObject]) -> None:
        """Register an Object type."""
        name = object_type.__name__
        self._registry[name] = object_type

    def list_types(self) -> list[str]:
        """List all registered Object type names."""
        return list(self._registry.keys())


class CosmosClient:
    """
    Main client for Cosmos SDK.

    Provides access to Objects and Links through an ORM-style API.

    Example:
        client = CosmosClient(token="eyJhbG...")

        # Get a single object
        customer = await client.objects.Customer.get("cust_123")

        # Query with filters
        customers = await client.objects.Customer.where(
            Customer.status == "active"
        ).list()

        # Convert to DataFrame
        df = await client.objects.Customer.where(
            Customer.status == "active"
        ).to_dataframe()
    """

    def __init__(
        self,
        token: str | None = None,
        base_url: str | None = None,
        timeout: float = 30.0,
    ):
        """
        Initialize Cosmos client.

        Args:
            token: JWT token for authentication. If not provided,
                   will try to read from COSMOS_AUTH_TOKEN environment variable.
            base_url: Base URL of ObjectDB service. If not provided,
                      will try to read from COSMOS_API_URL environment variable,
                      defaulting to http://localhost:8080.
            timeout: Request timeout in seconds.
        """
        self._token = token or os.environ.get("COSMOS_AUTH_TOKEN")
        self._base_url = base_url or os.environ.get("COSMOS_API_URL", "http://localhost:8080")
        self._timeout = timeout

        # Initialize API client
        self._api_client = ObjectDBClient(
            base_url=self._base_url,
            token=self._token,
            timeout=self._timeout,
        )

        # Object type registry
        self._object_registry: dict[str, type[BaseObject]] = {}

        # Load default objects from cosmos_sdk.objects
        self._load_default_objects()

        # Objects accessor
        self.objects = ObjectsAccessor(self, self._object_registry)

    def _load_default_objects(self) -> None:
        """Load default Object types from cosmos_sdk.objects."""
        try:
            from cosmos_sdk import objects

            for name in getattr(objects, "__all__", []):
                obj_class = getattr(objects, name, None)
                if obj_class and isinstance(obj_class, type) and issubclass(obj_class, BaseObject):
                    self._object_registry[name] = obj_class
        except ImportError:
            # No default objects available
            pass

    def register_objects(self, *object_types: type[BaseObject]) -> None:
        """
        Register additional Object types.

        Args:
            *object_types: Object classes to register
        """
        for obj_type in object_types:
            self._object_registry[obj_type.__name__] = obj_type

    async def health(self) -> dict[str, str]:
        """Check service health."""
        return await self._api_client.health()

    async def close(self) -> None:
        """Close the client and release resources."""
        await self._api_client.close()

    async def __aenter__(self) -> CosmosClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()


# Convenience function for creating a client
def create_client(
    token: str | None = None,
    base_url: str | None = None,
) -> CosmosClient:
    """
    Create a new Cosmos client.

    This is a convenience function equivalent to CosmosClient(...).

    Args:
        token: JWT token for authentication
        base_url: Base URL of ObjectDB service

    Returns:
        CosmosClient instance
    """
    return CosmosClient(token=token, base_url=base_url)
