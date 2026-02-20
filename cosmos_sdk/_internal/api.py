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
    ClearOverrideResult,
    CreateLinkTypeInput,
    CreateObjectInput,
    CreateObjectResult,
    CreateObjectTypeInput,
    Edge,
    FindPathsRequest,
    FindPathsResult,
    LinkType,
    ListLinkTypesResponse,
    ListObjectsResponse,
    ListObjectTypesResponse,
    ObjectAggregateRequest,
    ObjectAggregateResult,
    ObjectType,
    OverrideChange,
    OverrideResult,
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
        resp = await self._request(
            "GET",
            "/api/v1/object-types",
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return ListObjectTypesResponse(
            object_types=[ObjectType.model_validate(t) for t in resp.get("data", [])],
            count=resp.get("meta", {}).get("totalCount", len(resp.get("data", []))),
        )

    async def get_object_type(
        self,
        type_key: str,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ObjectType:
        """Get object type by key."""
        resp = await self._request(
            "GET",
            f"/api/v1/object-types/{type_key}",
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return ObjectType.model_validate(resp.get("data", resp))

    async def get_object_type_by_name(
        self,
        name: str,
        graph_key: str | None = None,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ObjectType | None:
        """Get object type by name (uses list endpoint with ?name= filter)."""
        try:
            resp = await self._request(
                "GET",
                "/api/v1/object-types",
                query={"tenantId": tenant_id, "name": name},
                jwt_token=jwt_token,
            )
            items = resp.get("data", [])
            if not items:
                return None
            return ObjectType.model_validate(items[0])
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
        resp = await self._request(
            "POST",
            "/api/v1/object-types",
            body=input.model_dump(exclude_none=True, by_alias=True),
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return ObjectType.model_validate(resp.get("data", resp))

    async def update_object_type(
        self,
        type_key: str,
        input: UpdateObjectTypeInput,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ObjectType:
        """Update an object type."""
        resp = await self._request(
            "PUT",
            f"/api/v1/object-types/{type_key}",
            body=input.model_dump(exclude_none=True, by_alias=True),
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return ObjectType.model_validate(resp.get("data", resp))

    async def delete_object_type(
        self,
        type_key: str,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> None:
        """Delete an object type."""
        await self._request(
            "DELETE",
            f"/api/v1/object-types/{type_key}",
            query={"tenantId": tenant_id},
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
        resp = await self._request(
            "GET",
            "/api/v1/link-types",
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return ListLinkTypesResponse(
            link_types=[LinkType.model_validate(lt) for lt in resp.get("data", [])],
            count=resp.get("meta", {}).get("totalCount", len(resp.get("data", []))),
        )

    async def get_link_type(
        self,
        link_type_key: str,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> LinkType:
        """Get link type by key."""
        resp = await self._request(
            "GET",
            f"/api/v1/link-types/{link_type_key}",
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return LinkType.model_validate(resp.get("data", resp))

    async def get_link_type_by_name(
        self,
        name: str,
        source_type: str | None = None,
        target_type: str | None = None,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> tuple[LinkType, bool] | None:
        """
        Get link type by name (uses list endpoint with ?name= filter).

        Returns:
            Tuple of (LinkType, reverse) or None if not found.
            reverse is True if the link is accessed from target side.
        """
        try:
            resp = await self._request(
                "GET",
                "/api/v1/link-types",
                query={
                    "tenantId": tenant_id,
                    "name": name,
                },
                jwt_token=jwt_token,
            )
            items = resp.get("data", [])
            if not items:
                return None

            # Find the best match considering source_type/target_type
            for item in items:
                lt = LinkType.model_validate(item)
                if source_type and lt.source_type == source_type:
                    return (lt, False)
                if target_type and lt.target_type == target_type:
                    return (lt, True)

            # No specific match, return first result
            lt = LinkType.model_validate(items[0])
            reverse = target_type is not None and lt.target_type == target_type
            return (lt, reverse)
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
        resp = await self._request(
            "POST",
            "/api/v1/link-types",
            body=input.model_dump(exclude_none=True, by_alias=True),
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return LinkType.model_validate(resp.get("data", resp))

    async def update_link_type(
        self,
        link_type_key: str,
        input: UpdateLinkTypeInput,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> LinkType:
        """Update a link type."""
        resp = await self._request(
            "PUT",
            f"/api/v1/link-types/{link_type_key}",
            body=input.model_dump(exclude_none=True, by_alias=True),
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return LinkType.model_validate(resp.get("data", resp))

    async def delete_link_type(
        self,
        link_type_key: str,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> None:
        """Delete a link type."""
        await self._request(
            "DELETE",
            f"/api/v1/link-types/{link_type_key}",
            query={"tenantId": tenant_id},
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
        resp = await self._request(
            "GET",
            f"/api/v1/objects/{object_type}/{object_id}",
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return ResolvedObject.model_validate(resp.get("data", resp))

    async def list_objects(
        self,
        object_type: str,
        limit: int | None = None,
        offset: int | None = None,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ListObjectsResponse:
        """List objects of a given type."""
        resp = await self._request(
            "GET",
            f"/api/v1/objects/{object_type}",
            query={
                "tenantId": tenant_id or "default",
                "limit": limit,
                "offset": offset,
            },
            jwt_token=jwt_token,
        )
        return ListObjectsResponse(
            objects=[ResolvedObject.model_validate(o) for o in resp.get("data", [])],
            count=resp.get("meta", {}).get("totalCount", len(resp.get("data", []))),
        )

    async def search_objects(
        self,
        object_type: str,
        query: SearchQuery,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> SearchResult:
        """Search objects with filters and text query."""
        resp = await self._request(
            "POST",
            f"/api/v1/objects/{object_type}/search",
            body=query.model_dump(exclude_none=True, by_alias=True),
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return SearchResult.model_validate(resp.get("data", resp))

    async def aggregate_objects(
        self,
        object_type: str,
        request: ObjectAggregateRequest,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ObjectAggregateResult:
        """Aggregate objects with sum, avg, min, max, count, group_by."""
        resp = await self._request(
            "POST",
            f"/api/v1/objects/{object_type}/aggregate",
            body=request.model_dump(exclude_none=True, by_alias=True),
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return ObjectAggregateResult.model_validate(resp.get("data", resp))

    async def batch_get_objects(
        self,
        object_type: str,
        input: BatchGetObjectsInput,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> BatchGetObjectsResponse:
        """Get multiple objects by IDs."""
        resp = await self._request(
            "POST",
            f"/api/v1/objects/{object_type}/batch",
            body=input.model_dump(),
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return BatchGetObjectsResponse.model_validate(resp.get("data", resp))

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
        resp = await self._request(
            "POST",
            "/api/v1/actions",
            body=input.model_dump(exclude_none=True, by_alias=True),
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return ActionEvent.model_validate(resp.get("data", resp))

    async def get_action_log(
        self,
        object_type: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ActionLogResponse:
        """Get action log."""
        resp = await self._request(
            "GET",
            "/api/v1/actions/log",
            query={
                "tenantId": tenant_id,
                "object_type": object_type,
                "limit": limit,
                "offset": offset,
            },
            jwt_token=jwt_token,
        )
        return ActionLogResponse.model_validate(resp.get("data", resp))

    # ========================================
    # Graph Traversal API
    # ========================================

    async def get_object_links(
        self,
        object_type: str,
        object_id: str,
        link_type: str | None = None,
        direction: str | None = None,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> list[Edge]:
        """Get links for an object."""
        resp = await self._request(
            "GET",
            f"/api/v1/objects/{object_type}/{object_id}/links",
            query={
                "tenantId": tenant_id,
                "link_type": link_type,
                "direction": direction,
            },
            jwt_token=jwt_token,
        )
        items = resp.get("data", [])
        return [Edge.model_validate(e) for e in items]

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
        resp = await self._request(
            "GET",
            f"/api/v1/objects/{object_type}/{object_id}/neighbors",
            query={
                "tenantId": tenant_id,
                "link_type": link_type,
                "direction": direction,
            },
            jwt_token=jwt_token,
        )
        items = resp.get("data", resp.get("objects", []))
        return [ResolvedObject.model_validate(o) for o in items]

    async def traverse(
        self,
        request: TraversalRequest,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> TraversalResult:
        """Traverse the object graph."""
        resp = await self._request(
            "POST",
            "/api/v1/graph/traverse",
            body=request.model_dump(exclude_none=True, by_alias=True),
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return TraversalResult.model_validate(resp.get("data", resp))

    async def find_paths(
        self,
        request: FindPathsRequest,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> FindPathsResult:
        """Find paths between object types (schema-level)."""
        resp = await self._request(
            "POST",
            "/api/v1/graph/find-paths",
            body=request.model_dump(exclude_none=True, by_alias=True),
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return FindPathsResult.model_validate(resp.get("data", resp))

    # ========================================
    # Override API (Object Actions)
    # ========================================

    async def override(
        self,
        object_type: str,
        object_ids: list[str],
        changes: list[OverrideChange],
        action_id: str | None = None,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> OverrideResult:
        """
        Apply override changes to objects.

        Used by Object Actions to modify object state through the ObjectDB Override API.

        Args:
            object_type: Type of the objects to modify
            object_ids: List of object IDs to apply changes to
            changes: List of OverrideChange specifications
            action_id: Optional action ID for audit trail
            tenant_id: Tenant ID (defaults to 'default')
            jwt_token: Override JWT token for this request

        Returns:
            OverrideResult with applied_count and updated_objects

        Example:
            result = await client.override(
                object_type="Order",
                object_ids=["order_123"],
                changes=[
                    OverrideChange(property="status", op="TRANSITION", value="APPROVED"),
                    OverrideChange(property="approvedAt", op="SET", value="2026-02-05T10:00:00Z")
                ],
                action_id="job_xxx"
            )
        """
        resp = await self._request(
            "POST",
            f"/api/v1/objects/{object_type}/override",
            body={
                "objectIds": object_ids,
                "changes": [c.model_dump(exclude_none=True, by_alias=True) for c in changes],
                "actionId": action_id,
            },
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return OverrideResult.model_validate(resp.get("data", resp))

    async def create_object(
        self,
        object_type: str,
        object_id: str,
        properties: dict,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> CreateObjectResult:
        """
        Create a new object.

        Args:
            object_type: Type of the object to create
            object_id: Unique ID for the new object
            properties: Initial property values
            tenant_id: Tenant ID (defaults to 'default')
            jwt_token: Override JWT token for this request

        Returns:
            CreateObjectResult with the created object info

        Example:
            result = await client.create_object(
                object_type="Order",
                object_id="order_001",
                properties={
                    "status": "REQUESTED",
                    "amount": 50000,
                    "requesterId": "user_123"
                }
            )
        """
        resp = await self._request(
            "POST",
            f"/api/v1/objects/{object_type}",
            body={
                "objectId": object_id,
                "properties": properties,
            },
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return CreateObjectResult.model_validate(resp.get("data", resp))

    async def clear_override(
        self,
        object_type: str,
        object_ids: list[str],
        properties: list[str],
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> ClearOverrideResult:
        """
        Clear overrides and revert to Fact values.

        Args:
            object_type: Type of the objects
            object_ids: List of object IDs
            properties: List of property names to clear overrides for
            tenant_id: Tenant ID (defaults to 'default')
            jwt_token: Override JWT token for this request

        Returns:
            ClearOverrideResult with cleared_count and updated_objects

        Example:
            result = await client.clear_override(
                object_type="Order",
                object_ids=["order_123"],
                properties=["status", "approvedAt"]
            )
        """
        resp = await self._request(
            "POST",
            f"/api/v1/objects/{object_type}/override/clear",
            body={
                "objectIds": object_ids,
                "properties": properties,
            },
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )
        return ClearOverrideResult.model_validate(resp.get("data", resp))

    async def delete_object(
        self,
        object_type: str,
        object_id: str,
        tenant_id: str | None = None,
        jwt_token: str | None = None,
    ) -> None:
        """
        Delete an object.

        Args:
            object_type: Type of the object to delete
            object_id: ID of the object to delete
            tenant_id: Tenant ID (defaults to 'default')
            jwt_token: Override JWT token for this request

        Example:
            await client.delete_object(
                object_type="Order",
                object_id="order_123"
            )
        """
        await self._request(
            "DELETE",
            f"/api/v1/objects/{object_type}/{object_id}",
            query={"tenantId": tenant_id},
            jwt_token=jwt_token,
        )

