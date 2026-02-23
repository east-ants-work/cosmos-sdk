"""
Pydantic models for ObjectDB API types.

These types mirror the Go domain models in ObjectDB service.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _to_camel(snake_str: str) -> str:
    """Convert snake_case to camelCase."""
    components = snake_str.split("_")
    return components[0] + "".join(x.capitalize() for x in components[1:])


class CamelModel(BaseModel):
    """Base model that accepts both camelCase and snake_case fields."""

    model_config = ConfigDict(
        alias_generator=_to_camel,
        populate_by_name=True,
    )


# ========================================
# Enums
# ========================================


class PropertyType(str, Enum):
    STRING = "string"
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    DATETIME = "datetime"
    JSON = "json"
    ARRAY = "array"


class PropertySource(str, Enum):
    DATASET = "dataset"
    ACTION = "action"
    DERIVED = "derived"


class AllowedOperation(str, Enum):
    SET = "SET"
    CLEAR_OVERRIDE = "CLEAR_OVERRIDE"
    INCREMENT = "INCREMENT"
    DECREMENT = "DECREMENT"
    APPEND = "APPEND"
    REMOVE = "REMOVE"
    TRANSITION = "TRANSITION"


class Cardinality(str, Enum):
    ONE_TO_ONE = "one-to-one"
    ONE_TO_MANY = "one-to-many"


class EdgeDirection(str, Enum):
    OUTGOING = "outgoing"
    INCOMING = "incoming"
    BOTH = "both"


class SearchOp(str, Enum):
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in"
    LIKE = "like"
    EXISTS = "exists"


class JobType(str, Enum):
    INDEXING = "indexing"
    REINDEXING = "reindexing"
    SYNC_CHECK = "sync_check"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ========================================
# ObjectType and Properties
# ========================================


class StateMachine(CamelModel):
    initial_state: str
    transitions: dict[str, list[str]]  # current_state -> allowed_next_states


class PropertyDefinition(CamelModel):
    name: str
    key: str | None = None
    type: PropertyType
    source: PropertySource
    required: bool = False
    indexed: bool = False
    allowed_ops: list[AllowedOperation] = Field(default_factory=list)
    default: Any | None = None
    state_machine: StateMachine | None = None
    expression: str | None = None
    description: str | None = None
    deprecated_at: str | None = None


class ObjectType(CamelModel):
    tenant_id: str | None = None
    type_key: str
    name: str
    description: str | None = None
    backing_dataset: str | None = None
    primary_key: str | None = None
    title_field: str | None = None
    properties: list[PropertyDefinition]
    created_at: str
    updated_at: str
    last_sync_snapshot_id: int | None = None
    last_sync_at: str | None = None


class CreateObjectTypeInput(CamelModel):
    type_key: str
    name: str
    description: str | None = None
    backing_dataset: str | None = None
    primary_key: str | None = None
    title_field: str | None = None
    properties: list[PropertyDefinition]


class UpdateObjectTypeInput(CamelModel):
    name: str | None = None
    description: str | None = None
    backing_dataset: str | None = None
    primary_key: str | None = None
    title_field: str | None = None
    properties: list[PropertyDefinition] | None = None


# ========================================
# LinkType and Edges
# ========================================


class ForeignKeyConfig(CamelModel):
    source_field: str
    target_pk_field: str
    auto_sync: bool = False


class LinkType(CamelModel):
    tenant_id: str | None = None
    link_type_key: str
    name: str
    source_type: str
    target_type: str
    cardinality: Cardinality
    properties: list[PropertyDefinition] | None = None
    fk_config: ForeignKeyConfig | None = None
    created_at: str
    updated_at: str | None = None


class CreateLinkTypeInput(CamelModel):
    link_type_key: str
    name: str
    source_type: str
    target_type: str
    cardinality: Cardinality
    properties: list[PropertyDefinition] | None = None
    fk_config: ForeignKeyConfig | None = None


class UpdateLinkTypeInput(CamelModel):
    name: str | None = None
    source_type: str | None = None
    target_type: str | None = None
    cardinality: Cardinality | None = None
    properties: list[PropertyDefinition] | None = None
    fk_config: ForeignKeyConfig | None = None


class Edge(CamelModel):
    tenant_id: str | None = None
    link_type: str
    source_id: str
    target_id: str
    properties: dict[str, Any] | None = None
    created_at: str


class CreateEdgeInput(CamelModel):
    link_type: str
    source_id: str
    target_id: str
    properties: dict[str, Any] | None = None


# ========================================
# ResolvedObject
# ========================================


class Override(CamelModel):
    value: Any
    override_ts: str
    actor: str
    action_id: str
    active: bool


class ResolvedObject(CamelModel):
    tenant_id: str | None = None
    object_type: str
    object_id: str
    effective_state: dict[str, Any]
    fact_state: dict[str, Any] | None = None
    overrides: dict[str, Override] | None = None
    version: int
    snapshot_id: int | None = None
    created_at: str
    updated_at: str


# ========================================
# Action Events
# ========================================


class PropertyChange(CamelModel):
    property: str
    op: AllowedOperation
    value: Any | None = None


class ActionEvent(CamelModel):
    event_id: str
    tenant_id: str | None = None
    object_type: str
    object_ids: list[str]
    changes: list[PropertyChange]
    actor: str
    submitted_at: str
    idempotency_key: str | None = None


class ApplyActionInput(CamelModel):
    object_type: str
    object_ids: list[str]
    changes: list[PropertyChange]
    idempotency_key: str | None = None


# ========================================
# Search
# ========================================


class SearchFilter(CamelModel):
    field: str
    op: SearchOp
    value: Any


class SearchQuery(CamelModel):
    query: str | None = None
    filters: list[SearchFilter] | None = None
    sort_by: str | None = None
    sort_order: Literal["asc", "desc"] | None = None
    limit: int | None = None
    offset: int | None = None


class SearchResult(CamelModel):
    objects: list[ResolvedObject]
    total: int
    limit: int
    offset: int


# ========================================
# Graph Traversal
# ========================================


class TraversalStep(CamelModel):
    link_type: str
    direction: EdgeDirection


class TraversalFilter(CamelModel):
    field: str
    op: str
    value: Any


class TraversalRequest(CamelModel):
    start_id: str
    start_type: str
    steps: list[TraversalStep]
    filters: dict[str, TraversalFilter] | None = None
    limit: int | None = None


class TraversalPath(CamelModel):
    nodes: list[str]
    edges: list[Edge]


class TraversalResult(CamelModel):
    paths: list[TraversalPath]
    objects: list[ResolvedObject]
    total: int


# ========================================
# Path Finding (Schema-level)
# ========================================


class FindPathsRequest(CamelModel):
    start_type: str
    end_type: str
    max_depth: int | None = None
    limit: int | None = None


class TypePathEdge(CamelModel):
    link_type_key: str
    name: str
    source_type: str
    target_type: str
    reverse: bool


class TypePath(CamelModel):
    nodes: list[str]
    edges: list[TypePathEdge]
    length: int


class FindPathsResult(CamelModel):
    paths: list[TypePath]
    total: int
    truncated: bool


# ========================================
# API Response Types
# ========================================


class ListObjectTypesResponse(CamelModel):
    object_types: list[ObjectType]
    count: int


class ListLinkTypesResponse(CamelModel):
    link_types: list[LinkType]
    count: int


class ListObjectsResponse(CamelModel):
    objects: list[ResolvedObject] = []
    count: int = 0

    @field_validator("objects", mode="before")
    @classmethod
    def objects_default(cls, v: Any) -> list[ResolvedObject]:
        """Convert None to empty list."""
        return v if v is not None else []

    @property
    def total(self) -> int:
        """Alias for count for compatibility."""
        return self.count


class BatchGetObjectsInput(CamelModel):
    object_ids: list[str]


class BatchGetObjectsResponse(CamelModel):
    objects: list[ResolvedObject]


class ActionLogResponse(CamelModel):
    events: list[ActionEvent]
    total: int


# ========================================
# Object Aggregation
# ========================================


class MetricRequest(CamelModel):
    """Single metric aggregation request."""
    name: str  # Output field name
    type: str  # sum | avg | min | max | count | stats
    field: str = ""  # Field to aggregate (not required for count)


class ObjectAggregateRequest(CamelModel):
    """Request for object data aggregation."""
    filters: list[SearchFilter] | None = None
    group_by: list[str] | None = None
    metrics: list[MetricRequest]


class ObjectAggregateResult(CamelModel):
    """Result of object aggregation."""
    buckets: list[dict[str, Any]] | None = None
    metrics: dict[str, Any] | None = None
    total: int = 0


# ========================================
# ClickHouse Analytics Aggregation
# ========================================


class CHMetric(CamelModel):
    """ClickHouse aggregation metric."""
    name: str  # Result column name
    function: Literal["count", "sum", "avg", "min", "max"]
    field: str | None = None  # Field to aggregate (not required for count)


class CHFilter(CamelModel):
    """ClickHouse aggregation filter."""
    field: str
    operator: Literal["eq", "ne", "gt", "gte", "lt", "lte", "in", "like"]
    value: Any


class CHAggregateRequest(CamelModel):
    """Request for ClickHouse analytics aggregation."""
    object_type: str
    group_by: list[str] | None = None
    metrics: list[CHMetric]
    filters: list[CHFilter] | None = None
    order_by: str | None = None
    order_dir: Literal["asc", "desc"] | None = None
    limit: int | None = None


class CHAggregateResult(CamelModel):
    """Result of ClickHouse analytics aggregation."""
    rows: list[dict[str, Any]]
    total: int = 0


# ========================================
# Object Action Override API
# ========================================


class OverrideChange(CamelModel):
    """
    Change specification for ObjectDB Override API.

    Used by Object Actions to apply state transitions.

    Example:
        OverrideChange(property="status", op="TRANSITION", value="APPROVED")
        OverrideChange(property="approvedAt", op="SET", value=datetime.now().isoformat())
    """
    property: str
    op: AllowedOperation
    value: Any | None = None


class OverrideResult(CamelModel):
    """Result of an override operation."""
    applied_count: int
    updated_objects: list[ResolvedObject] = Field(default_factory=list)
    errors: list[dict[str, Any]] = Field(default_factory=list)


class CreateObjectInput(CamelModel):
    """Input for creating a new object."""
    object_id: str
    properties: dict[str, Any]


class CreateObjectResult(CamelModel):
    """Result of object creation."""
    tenant_id: str | None = None
    object_type: str
    object_id: str
    effective_state: dict[str, Any]
    version: int
    created_at: str


class ClearOverrideInput(CamelModel):
    """Input for clearing overrides."""
    object_type: str
    object_ids: list[str]
    properties: list[str]


class ClearOverrideResult(CamelModel):
    """Result of clearing overrides."""
    cleared_count: int
    updated_objects: list[ResolvedObject] = Field(default_factory=list)
