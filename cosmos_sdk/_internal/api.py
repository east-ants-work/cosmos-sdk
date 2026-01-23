"""
Low-level HTTP client for ObjectDB service.
"""

from __future__ import annotations

import logging
from typing import Any, TypeVar
from urllib.parse import urlencode

import httpx

from cosmos_sdk._internal.types import (
    ActionEvent,
    ActionLogResponse,
    ApplyActionInput,
    BatchGetObjectsInput,
    BatchGetObjectsResponse,
    CreateEdgeInput,
    CreateLinkTypeInput,
    CreateObjectTypeInput,
    Edge,
    FindPathsRequest,
    FindPathsResult,
    LinkType,
    ListLinkTypesResponse,
    ListObjectsResponse,
    ListObjectTypesResponse,
    ObjectType,
    ResolvedObject,
    SearchQuery,
    SearchResult,
    TraversalRequest,
    TraversalResult,
    UpdateLinkTypeInput,
    UpdateObjectTypeInput,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ObjectDBError(Exception):
    """Base exception for ObjectDB errors."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class ObjectDBClient:
    """
    Low-level HTTP client for ObjectDB service.

    This client provides direct access to all ObjectDB API endpoints.
    For ORM-style access, use CosmosClient instead.
    """

    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        timeout: float = 30.0,
    ):
        """
        Initialize ObjectDB client.

        Args:
            base_url: Base URL of ObjectDB service (e.g., "http://localhost:8080")
            token: JWT token for authentication
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> ObjectDBClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    def _build_url(
        self,
        path: str,
        query: dict[str, str | int | None] | None = None,
    ) -> str:
        """Build full URL with query parameters."""
        url = f"{self.base_url}{path}"
        if query:
            # Filter out None values
            filtered = {k: str(v) for k, v in query.items() if v is not None}
            if filtered:
                url = f"{url}?{urlencode(filtered)}"
        return url

    def _build_headers(self, jwt_token: str | None = None) -> dict[str, str]:
        """Build request headers with authentication."""
        headers = {"Content-Type": "application/json"}
        token = jwt_token or self.token
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        *,
        body: Any | None = None,
        query: dict[str, str | int | None] | None = None,
        jwt_token: str | None = None,
    ) -> Any:
        """
        Make an HTTP request to ObjectDB.

        Args:
            method: HTTP method
            path: URL path
            body: Request body (will be serialized to JSON)
            query: Query parameters
            jwt_token: Override JWT token for this request

        Returns:
            Parsed JSON response

        Raises:
            ObjectDBError: On request failure
        """
        client = await self._get_client()
        url = self._build_url(path, query)
        headers = self._build_headers(jwt_token)

        try:
            response = await client.request(
                method,
                url,
                headers=headers,
                json=body,
            )

            if not response.is_success:
                error_body = response.text
                logger.error(
                    "ObjectDB request failed: %s %s - %d: %s",
                    method,
                    path,
                    response.status_code,
                    error_body,
                )
                raise ObjectDBError(
                    f"ObjectDB request failed: {response.status_code} {response.reason_phrase}",
                    status_code=response.status_code,
                )

            # Handle 204 No Content
            if response.status_code == 204:
                return None

            return response.json()

        except httpx.TimeoutException as e:
            raise ObjectDBError(f"ObjectDB request timeout: {method} {path}") from e

    # ========================================
    # Health Check
    # ========================================

    async def health(self) -> dict[str, str]:
        """Check service health."""
        return await self._request("GET", "/health")

    async def ready(self) -> dict[str, str]:
        """Check service readiness."""
        return await self._request("GET", "/ready")

    # ========================================
    # ObjectType API
    # ========================================

    async def list_object_types(
        self,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ListObjectTypesResponse:
        """List all object types."""
        data = await self._request(
            "GET",
            "/api/v1/types",
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return ListObjectTypesResponse.model_validate(data)

    async def list_object_types_internal(
        self,
        tenant_id: str | None = None,
    ) -> ListObjectTypesResponse:
        """List object types using internal endpoint (no auth)."""
        data = await self._request(
            "GET",
            "/internal/types",
            query={"tenant_id": tenant_id},
        )
        return ListObjectTypesResponse.model_validate(data)

    async def get_object_type(
        self,
        type_key: str,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ObjectType:
        """Get object type by key."""
        data = await self._request(
            "GET",
            f"/api/v1/types/{type_key}",
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return ObjectType.model_validate(data)

    async def get_object_type_by_name(
        self,
        name: str,
        graph_key: str | None = None,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ObjectType | None:
        """Get object type by name."""
        try:
            data = await self._request(
                "GET",
                f"/api/v1/types/by-name/{name}",
                query={"tenant_id": tenant_id, "graph_key": graph_key},
                jwt_token=jwt_token,
            )
            return ObjectType.model_validate(data)
        except ObjectDBError as e:
            if e.status_code == 404:
                return None
            raise

    async def get_object_type_by_name_internal(
        self,
        name: str,
        graph_key: str | None = None,
        tenant_id: str | None = None,
    ) -> ObjectType | None:
        """Get object type by name using internal endpoint."""
        try:
            data = await self._request(
                "GET",
                f"/internal/types/by-name/{name}",
                query={"tenant_id": tenant_id, "graph_key": graph_key},
            )
            return ObjectType.model_validate(data)
        except ObjectDBError as e:
            if e.status_code == 404:
                return None
            raise

    async def create_object_type(
        self,
        input: CreateObjectTypeInput,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ObjectType:
        """Create a new object type."""
        data = await self._request(
            "POST",
            "/api/v1/types",
            body=input.model_dump(exclude_none=True),
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return ObjectType.model_validate(data)

    async def update_object_type(
        self,
        type_key: str,
        input: UpdateObjectTypeInput,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ObjectType:
        """Update an object type."""
        data = await self._request(
            "PUT",
            f"/api/v1/types/{type_key}",
            body=input.model_dump(exclude_none=True),
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return ObjectType.model_validate(data)

    async def delete_object_type(
        self,
        type_key: str,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> None:
        """Delete an object type."""
        await self._request(
            "DELETE",
            f"/api/v1/types/{type_key}",
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )

    # ========================================
    # LinkType API
    # ========================================

    async def list_link_types(
        self,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ListLinkTypesResponse:
        """List all link types."""
        data = await self._request(
            "GET",
            "/api/v1/links",
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return ListLinkTypesResponse.model_validate(data)

    async def get_link_type(
        self,
        link_type_key: str,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> LinkType:
        """Get link type by key."""
        data = await self._request(
            "GET",
            f"/api/v1/links/{link_type_key}",
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return LinkType.model_validate(data)

    async def get_link_type_by_name(
        self,
        name: str,
        source_type: str | None = None,
        target_type: str | None = None,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> tuple[LinkType, bool] | None:
        """
        Get link type by name.

        Returns:
            Tuple of (LinkType, reverse) or None if not found.
            reverse is True if the link is accessed from target side.
        """
        try:
            data = await self._request(
                "GET",
                f"/api/v1/links/by-name/{name}",
                query={
                    "tenant_id": tenant_id,
                    "source_type": source_type,
                    "target_type": target_type,
                },
                jwt_token=jwt_token,
            )
            return (
                LinkType.model_validate(data["link_type"]),
                data.get("reverse", False),
            )
        except ObjectDBError as e:
            if e.status_code == 404:
                return None
            raise

    async def get_link_type_by_name_internal(
        self,
        name: str,
        source_type: str | None = None,
        target_type: str | None = None,
        tenant_id: str | None = None,
    ) -> tuple[LinkType, bool] | None:
        """Get link type by name using internal endpoint."""
        try:
            data = await self._request(
                "GET",
                f"/internal/links/by-name/{name}",
                query={
                    "tenant_id": tenant_id,
                    "source_type": source_type,
                    "target_type": target_type,
                },
            )
            return (
                LinkType.model_validate(data["link_type"]),
                data.get("reverse", False),
            )
        except ObjectDBError as e:
            if e.status_code == 404:
                return None
            raise

    async def create_link_type(
        self,
        input: CreateLinkTypeInput,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> LinkType:
        """Create a new link type."""
        data = await self._request(
            "POST",
            "/api/v1/links",
            body=input.model_dump(exclude_none=True),
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return LinkType.model_validate(data)

    async def update_link_type(
        self,
        link_type_key: str,
        input: UpdateLinkTypeInput,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> LinkType:
        """Update a link type."""
        data = await self._request(
            "PUT",
            f"/api/v1/links/{link_type_key}",
            body=input.model_dump(exclude_none=True),
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return LinkType.model_validate(data)

    async def delete_link_type(
        self,
        link_type_key: str,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> None:
        """Delete a link type."""
        await self._request(
            "DELETE",
            f"/api/v1/links/{link_type_key}",
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )

    # ========================================
    # Objects API
    # ========================================

    async def get_object(
        self,
        object_type: str,
        object_id: str,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ResolvedObject:
        """Get a single object by ID."""
        data = await self._request(
            "GET",
            f"/api/v1/objects/{object_type}/{object_id}",
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return ResolvedObject.model_validate(data)

    async def list_objects(
        self,
        object_type: str,
        limit: int | None = None,
        offset: int | None = None,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ListObjectsResponse:
        """List objects of a given type."""
        data = await self._request(
            "GET",
            f"/api/v1/objects/{object_type}",
            query={
                "tenant_id": tenant_id,
                "limit": limit,
                "offset": offset,
            },
            jwt_token=jwt_token,
        )
        return ListObjectsResponse.model_validate(data)

    async def search_objects(
        self,
        object_type: str,
        query: SearchQuery,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> SearchResult:
        """Search objects with filters and text query."""
        data = await self._request(
            "POST",
            f"/api/v1/objects/{object_type}/search",
            body=query.model_dump(exclude_none=True),
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return SearchResult.model_validate(data)

    async def batch_get_objects(
        self,
        object_type: str,
        input: BatchGetObjectsInput,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> BatchGetObjectsResponse:
        """Get multiple objects by IDs."""
        data = await self._request(
            "POST",
            f"/api/v1/objects/{object_type}/batch",
            body=input.model_dump(),
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return BatchGetObjectsResponse.model_validate(data)

    # ========================================
    # Actions API
    # ========================================

    async def apply_action(
        self,
        input: ApplyActionInput,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ActionEvent:
        """Apply an action to objects."""
        data = await self._request(
            "POST",
            "/api/v1/actions",
            body=input.model_dump(exclude_none=True),
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return ActionEvent.model_validate(data)

    async def get_action_log(
        self,
        object_type: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ActionLogResponse:
        """Get action log."""
        data = await self._request(
            "GET",
            "/api/v1/actions/log",
            query={
                "tenant_id": tenant_id,
                "object_type": object_type,
                "limit": limit,
                "offset": offset,
            },
            jwt_token=jwt_token,
        )
        return ActionLogResponse.model_validate(data)

    # ========================================
    # Edges API
    # ========================================

    async def create_edge(
        self,
        input: CreateEdgeInput,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> Edge:
        """Create an edge between objects."""
        data = await self._request(
            "POST",
            "/api/v1/edges",
            body=input.model_dump(exclude_none=True),
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return Edge.model_validate(data)

    async def delete_edge(
        self,
        link_type: str,
        source_id: str,
        target_id: str,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> None:
        """Delete an edge between objects."""
        await self._request(
            "DELETE",
            f"/api/v1/edges/{link_type}/{source_id}/{target_id}",
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )

    # ========================================
    # Graph Traversal API
    # ========================================

    async def get_object_edges(
        self,
        object_type: str,
        object_id: str,
        link_type: str | None = None,
        direction: str | None = None,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> list[Edge]:
        """Get edges for an object."""
        data = await self._request(
            "GET",
            f"/api/v1/objects/{object_type}/{object_id}/edges",
            query={
                "tenant_id": tenant_id,
                "link_type": link_type,
                "direction": direction,
            },
            jwt_token=jwt_token,
        )
        return [Edge.model_validate(e) for e in data.get("edges", [])]

    async def get_object_neighbors(
        self,
        object_type: str,
        object_id: str,
        link_type: str | None = None,
        direction: str | None = None,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> list[ResolvedObject]:
        """Get neighboring objects."""
        data = await self._request(
            "GET",
            f"/api/v1/objects/{object_type}/{object_id}/neighbors",
            query={
                "tenant_id": tenant_id,
                "link_type": link_type,
                "direction": direction,
            },
            jwt_token=jwt_token,
        )
        return [ResolvedObject.model_validate(o) for o in data.get("objects", [])]

    async def traverse(
        self,
        request: TraversalRequest,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> TraversalResult:
        """Traverse the object graph."""
        data = await self._request(
            "POST",
            "/api/v1/graph/traverse",
            body=request.model_dump(exclude_none=True),
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return TraversalResult.model_validate(data)

    async def find_paths(
        self,
        request: FindPathsRequest,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> FindPathsResult:
        """Find paths between object types (schema-level)."""
        data = await self._request(
            "POST",
            "/api/v1/graph/find-paths",
            body=request.model_dump(exclude_none=True),
            query={"tenant_id": tenant_id},
            jwt_token=jwt_token,
        )
        return FindPathsResult.model_validate(data)
