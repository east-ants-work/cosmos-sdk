"""
SparkClient — Read-only Cosmos SDK client for PySpark environments.

Provides the same `client.objects.Customer` pattern as CosmosClient,
but returns PySpark DataFrames instead of Polars DataFrames.

Usage:
    from cosmos_sdk.spark import SparkClient

    client = SparkClient(spark=spark, backend="iceberg", object_metadata={...})
    df = client.objects.Customer.to_dataframe()
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from cosmos_sdk.base import BaseObject
from cosmos_sdk.spark.object_set import SparkObjectSet

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class SparkObjectsAccessor:
    """
    Provides attribute-style access to SparkObjectSet instances.

    Looks up BaseObject subclasses to find __object_type__ / __object_type_key__,
    then creates a SparkObjectSet for the matching type.
    """

    def __init__(self, client: SparkClient):
        self._client = client
        self._registry: dict[str, tuple[str, str]] | None = None

    def _build_registry(self) -> dict[str, tuple[str, str]]:
        """Build a name -> (type, type_key) mapping from BaseObject subclasses."""
        registry: dict[str, tuple[str, str]] = {}
        for cls in BaseObject.__subclasses__():
            obj_type = getattr(cls, "__object_type__", "") or cls.__name__
            obj_type_key = getattr(cls, "__object_type_key__", "") or obj_type.lower()
            # Register by class name
            registry[cls.__name__] = (obj_type, obj_type_key)
            # Also register by __object_type__ if different
            if obj_type and obj_type != cls.__name__:
                registry[obj_type] = (obj_type, obj_type_key)
        return registry

    def __getattr__(self, name: str) -> SparkObjectSet:
        if name.startswith("_"):
            raise AttributeError(name)

        if self._registry is None:
            self._registry = self._build_registry()

        if name in self._registry:
            obj_type, obj_type_key = self._registry[name]
        else:
            # Fallback: use name as both type and type_key
            obj_type = name
            obj_type_key = name.lower()

        return SparkObjectSet(
            spark=self._client._spark,
            object_type=obj_type,
            object_type_key=obj_type_key,
            backend=self._client._backend,
            base_url=self._client._base_url,
            token=self._client._token,
            object_metadata=self._client._object_metadata,
            security_rules=self._client._security_rules,
            timeout=self._client._timeout,
        )


class SparkClient:
    """
    Read-only Cosmos SDK client for PySpark environments.

    Args:
        spark: PySpark SparkSession instance
        base_url: Cosmos API or ObjectDB URL (falls back to COSMOS_API_URL env var)
        token: Auth token (falls back to COSMOS_AUTH_TOKEN env var)
        backend: Data access backend — "api", "objectdb", or "iceberg"
        object_metadata: Pre-resolved object metadata from executor config
        security_rules: Security rules from executor config
        timeout: HTTP request timeout in seconds
    """

    def __init__(
        self,
        spark: SparkSession,
        base_url: str | None = None,
        token: str | None = None,
        backend: str = "iceberg",
        object_metadata: dict[str, Any] | None = None,
        security_rules: dict[str, Any] | None = None,
        timeout: float = 30.0,
    ):
        self._spark = spark
        self._backend = backend
        self._base_url = base_url or os.environ.get("COSMOS_API_URL")
        self._token = token or os.environ.get("COSMOS_AUTH_TOKEN")
        self._object_metadata = object_metadata or {}
        self._security_rules = security_rules or {}
        self._timeout = timeout
        self.objects = SparkObjectsAccessor(self)
