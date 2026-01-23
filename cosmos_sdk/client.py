"""
Cosmos SDK Client.

Main entry point for interacting with the Cosmos Object Graph.
"""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Any, TypeVar

from cosmos_sdk._internal.api import ObjectDBClient
from cosmos_sdk.base import BaseObject, ObjectSet

if TYPE_CHECKING:
    pass

T = TypeVar("T", bound=BaseObject)

# Singleton instance
_client_instance: "CosmosClient | None" = None


class ObjectsAccessor:
    """
    Accessor for Object types with lazy loading.

    Provides attribute-style access to Object types:
        client.objects.Customer
        client.objects.Order

    Objects are loaded on-demand from the SDK path when first accessed.
    """

    def __init__(self, client: CosmosClient, object_registry: dict[str, type[BaseObject]]):
        self._client = client
        self._registry = object_registry

    _last_load_debug: list[str] = []  # Class-level debug storage

    def _try_load_from_sdk(self, name: str) -> type[BaseObject] | None:
        """Try to load an Object class from the graph SDK directory."""
        import importlib

        # Reset debug info
        self._last_load_debug = []
        debug = self._last_load_debug

        graph_key = os.environ.get("COSMOS_GRAPH_KEY", "")
        debug.append(f"load_attempt_for='{name}'")

        if not graph_key:
            debug.append("GRAPH_KEY_NOT_SET")
            return None

        sdk_base_path = os.environ.get("COSMOS_SDK_PATH", "/shared/python-sdk")
        safe_graph_key = graph_key.replace("/", "_").replace("\\", "_").replace("..", "_")
        sdk_path = os.path.join(sdk_base_path, safe_graph_key)

        if not os.path.exists(sdk_path):
            debug.append(f"SDK_PATH_NOT_EXISTS={sdk_path}")
            return None

        # Add SDK base path to sys.path if needed
        if sdk_base_path not in sys.path:
            sys.path.insert(0, sdk_base_path)
            debug.append("ADDED_TO_SYS_PATH")

        # Try to import the class from the graph module
        try:
            debug.append(f"importing_module='{safe_graph_key}'")
            graph_module = importlib.import_module(safe_graph_key)
            debug.append(f"module_imported={graph_module}")

            exported = getattr(graph_module, "__all__", [])
            debug.append(f"module_all={exported}")

            # Check if the class is exported
            if name in exported:
                obj_class = getattr(graph_module, name, None)
                debug.append(f"found_class={obj_class}")
                if obj_class and isinstance(obj_class, type) and issubclass(obj_class, BaseObject):
                    debug.append("SUCCESS_EXACT_MATCH")
                    return obj_class
                else:
                    debug.append(f"NOT_BASEOBJECT: is_type={isinstance(obj_class, type)}")

            # Try case-insensitive match
            for exported_name in exported:
                if exported_name.lower() == name.lower():
                    obj_class = getattr(graph_module, exported_name, None)
                    debug.append(f"found_case_insensitive={exported_name}->{obj_class}")
                    if obj_class and isinstance(obj_class, type) and issubclass(obj_class, BaseObject):
                        debug.append("SUCCESS_CASE_INSENSITIVE")
                        return obj_class

            debug.append(f"NOT_FOUND_IN_EXPORTS")

        except ImportError as e:
            debug.append(f"IMPORT_ERROR={e}")
        except Exception as e:
            debug.append(f"UNEXPECTED_ERROR={type(e).__name__}:{e}")

        return None

    def __getattr__(self, name: str) -> ObjectSet:
        """Get ObjectSet for the named type (lazy loading)."""
        import logging
        logger = logging.getLogger("cosmos_sdk.client")

        if name.startswith("_"):
            raise AttributeError(name)

        logger.debug(f"[ObjectsAccessor] __getattr__ called for '{name}'")
        logger.debug(f"[ObjectsAccessor] Current registry keys: {list(self._registry.keys())}")

        # Check registry first
        if name in self._registry:
            object_type = self._registry[name]
            return ObjectSet(
                self._client,
                object_type,
                getattr(object_type, '__object_type__', '') or name.lower(),
            )

        # Try to load from SDK (lazy loading)
        object_type = self._try_load_from_sdk(name)
        if object_type:
            # Cache in registry for future access
            self._registry[name] = object_type
            return ObjectSet(
                self._client,
                object_type,
                getattr(object_type, '__object_type__', '') or name.lower(),
            )

        # Try case-insensitive match in registry
        for key in self._registry:
            if key.lower() == name.lower():
                object_type = self._registry[key]
                return ObjectSet(
                    self._client,
                    object_type,
                    getattr(object_type, '__object_type__', '') or key.lower(),
                )

        # Collect debug info for error message
        graph_key = os.environ.get("COSMOS_GRAPH_KEY", "")
        sdk_base_path = os.environ.get("COSMOS_SDK_PATH", "/shared/python-sdk")
        safe_graph_key = graph_key.replace("/", "_").replace("\\", "_").replace("..", "_") if graph_key else ""
        sdk_path = os.path.join(sdk_base_path, safe_graph_key) if safe_graph_key else ""

        debug_info = []
        debug_info.append(f"COSMOS_GRAPH_KEY='{graph_key}'")
        debug_info.append(f"COSMOS_SDK_PATH='{sdk_base_path}'")
        debug_info.append(f"sdk_path='{sdk_path}'")
        debug_info.append(f"sdk_path_exists={os.path.exists(sdk_path) if sdk_path else False}")

        if sdk_path and os.path.exists(sdk_path):
            try:
                debug_info.append(f"sdk_contents={os.listdir(sdk_path)}")
            except Exception as e:
                debug_info.append(f"sdk_contents_error={e}")

        if sdk_base_path and os.path.exists(sdk_base_path):
            try:
                debug_info.append(f"sdk_base_contents={os.listdir(sdk_base_path)}")
            except Exception:
                pass

        debug_info.append(f"sys.path[0:5]={sys.path[0:5]}")
        debug_info.append(f"registry_keys={list(self._registry.keys())}")
        debug_info.append(f"load_debug=[{', '.join(self._last_load_debug)}]")

        raise AttributeError(
            f"Unknown Object type: {name}. "
            f"Debug: {'; '.join(debug_info)}"
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
    Main client for Cosmos SDK (Singleton).

    Provides access to Objects and Links through an ORM-style API.
    Uses singleton pattern - calling CosmosClient() returns the same instance.

    Example:
        client = CosmosClient()  # Uses env vars for config

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

    _initialized: bool = False

    def __new__(
        cls,
        token: str | None = None,
        base_url: str | None = None,
        timeout: float = 30.0,
    ) -> "CosmosClient":
        """Return singleton instance (create if needed)."""
        global _client_instance
        if _client_instance is None:
            _client_instance = super().__new__(cls)
            _client_instance._initialized = False
        return _client_instance

    def __init__(
        self,
        token: str | None = None,
        base_url: str | None = None,
        timeout: float = 30.0,
    ):
        """
        Initialize Cosmos client (singleton - only initializes once).

        Args:
            token: JWT token for authentication. If not provided,
                   will try to read from COSMOS_AUTH_TOKEN environment variable.
            base_url: Base URL of ObjectDB service. If not provided,
                      will try to read from COSMOS_API_URL environment variable,
                      defaulting to http://localhost:8080.
            timeout: Request timeout in seconds.
        """
        # Singleton: only initialize once
        if getattr(self, "_initialized", False):
            return

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

        self._initialized = True

    def _load_default_objects(self) -> None:
        """Load default Object types from cosmos_sdk.objects."""
        try:
            import importlib
            from cosmos_sdk import objects

            # Force reload to pick up COSMOS_GRAPH_KEY changes
            importlib.reload(objects)

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
