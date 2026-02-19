"""
Base classes for Cosmos SDK.

Provides ORM-style classes for defining and interacting with Objects and Links.
"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterator,
    Literal,
    Self,
    TypeVar,
    overload,
)

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    pl = None  # type: ignore[assignment]
    HAS_POLARS = False

if TYPE_CHECKING:
    from cosmos_sdk.client import CosmosClient
    from cosmos_sdk._internal.types import ResolvedObject, SearchFilter

T = TypeVar("T", bound="BaseObject")


# ========================================
# Property Descriptor
# ========================================


class PropertyComparison:
    """Represents a comparison operation for filtering."""

    def __init__(self, field: str, op: str, value: Any):
        self.field = field
        self.op = op
        self.value = value

    def __and__(self, other: PropertyComparison) -> CompositeFilter:
        return CompositeFilter("and", [self, other])

    def __or__(self, other: PropertyComparison) -> CompositeFilter:
        return CompositeFilter("or", [self, other])

    def __invert__(self) -> PropertyComparison:
        """Negate the comparison."""
        negation_map = {
            "eq": "ne",
            "ne": "eq",
            "gt": "lte",
            "gte": "lt",
            "lt": "gte",
            "lte": "gt",
        }
        new_op = negation_map.get(self.op, self.op)
        return PropertyComparison(self.field, new_op, self.value)

    def to_filter(self) -> SearchFilter:
        """Convert to SearchFilter for API call."""
        from cosmos_sdk._internal.types import SearchFilter, SearchOp

        return SearchFilter(field=self.field, op=SearchOp(self.op), value=self.value)


class CompositeFilter:
    """Represents a composite filter (AND/OR)."""

    def __init__(
        self, operator: Literal["and", "or"], conditions: list[PropertyComparison | CompositeFilter]
    ):
        self.operator = operator
        self.conditions = conditions

    def __and__(self, other: PropertyComparison | CompositeFilter) -> CompositeFilter:
        if self.operator == "and":
            return CompositeFilter("and", [*self.conditions, other])
        return CompositeFilter("and", [self, other])

    def __or__(self, other: PropertyComparison | CompositeFilter) -> CompositeFilter:
        if self.operator == "or":
            return CompositeFilter("or", [*self.conditions, other])
        return CompositeFilter("or", [self, other])


class Property:
    """
    Descriptor for Object properties.

    Supports operator overloading for building filters:
        Customer.status == "active"
        Customer.created_at >= "2024-01-01"
    """

    def __init__(
        self,
        type: str = "string",
        primary_key: bool = False,
        required: bool = False,
        indexed: bool = False,
        description: str | None = None,
    ):
        self.type = type
        self.primary_key = primary_key
        self.required = required
        self.indexed = indexed
        self.description = description
        self._name: str | None = None

    def __set_name__(self, owner: type, name: str) -> None:
        self._name = name

    @overload
    def __get__(self, obj: None, objtype: type) -> Property: ...

    @overload
    def __get__(self, obj: BaseObject, objtype: type) -> Any: ...

    def __get__(self, obj: BaseObject | None, objtype: type) -> Property | Any:
        if obj is None:
            # Class-level access for filtering
            return self
        # Instance-level access for value
        return obj._data.get(self._name)

    def __set__(self, obj: BaseObject, value: Any) -> None:
        obj._data[self._name] = value

    # Comparison operators for filtering
    def __eq__(self, other: Any) -> PropertyComparison:  # type: ignore[override]
        return PropertyComparison(self._name, "eq", other)

    def __ne__(self, other: Any) -> PropertyComparison:  # type: ignore[override]
        return PropertyComparison(self._name, "ne", other)

    def __gt__(self, other: Any) -> PropertyComparison:
        return PropertyComparison(self._name, "gt", other)

    def __ge__(self, other: Any) -> PropertyComparison:
        return PropertyComparison(self._name, "gte", other)

    def __lt__(self, other: Any) -> PropertyComparison:
        return PropertyComparison(self._name, "lt", other)

    def __le__(self, other: Any) -> PropertyComparison:
        return PropertyComparison(self._name, "lte", other)

    def is_null(self) -> PropertyComparison:
        """Check if property is null."""
        return PropertyComparison(self._name, "exists", False)

    def is_not_null(self) -> PropertyComparison:
        """Check if property is not null."""
        return PropertyComparison(self._name, "exists", True)

    def is_in(self, values: list[Any]) -> PropertyComparison:
        """Check if property value is in a list."""
        return PropertyComparison(self._name, "in", values)

    def like(self, pattern: str) -> PropertyComparison:
        """Check if property matches a pattern (SQL LIKE)."""
        return PropertyComparison(self._name, "like", pattern)


# ========================================
# Link Descriptor
# ========================================


class Link(Generic[T]):
    """
    Descriptor for Object links (relationships).

    Supports lazy loading of related objects:
        order.customer  # Returns Customer object
        order.products  # Returns list of Product objects
    """

    def __init__(
        self,
        target_type: str,
        many: bool = False,
        reverse: str | None = None,
        description: str | None = None,
    ):
        """
        Initialize a Link.

        Args:
            target_type: Name of the target Object type
            many: True for one-to-many or many-to-many relationships
            reverse: Name of the reverse link on target type
            description: Human-readable description
        """
        self.target_type = target_type
        self.many = many
        self.reverse = reverse
        self.description = description
        self._name: str | None = None

    def __set_name__(self, owner: type, name: str) -> None:
        self._name = name

    @overload
    def __get__(self, obj: None, objtype: type) -> Link[T]: ...

    @overload
    def __get__(self, obj: BaseObject, objtype: type) -> T | list[T]: ...

    def __get__(self, obj: BaseObject | None, objtype: type) -> Link[T] | T | list[T]:
        if obj is None:
            # Class-level access
            return self

        # Check cache first
        cache_key = f"_link_{self._name}"
        cached = obj._data.get(cache_key)
        if cached is not None:
            return cached

        # Lazy load from API
        if obj._client is None:
            raise RuntimeError("Cannot access link without a client")

        # Load synchronously (for convenience in non-async contexts)
        import asyncio

        try:
            loop = asyncio.get_running_loop()
            # We're in an async context, can't block
            raise RuntimeError(
                f"Cannot lazy-load link '{self._name}' in async context. "
                f"Use 'await obj.load_link(\"{self._name}\")' or eager loading with include=[\"{self._name}\"]"
            )
        except RuntimeError:
            # No running loop, safe to run synchronously
            pass

        result = asyncio.get_event_loop().run_until_complete(
            self._load_link(obj)
        )

        # Cache the result
        obj._data[cache_key] = result
        return result

    async def _load_link(self, obj: BaseObject) -> T | list[T]:
        """Load linked objects from API."""
        api_client = obj._client._api_client

        # Get neighbors via the link
        neighbors = await api_client.get_object_neighbors(
            obj.__object_type_key__,
            obj.object_id,
            link_type=self._name,
            direction="outgoing",
        )

        if not neighbors:
            return [] if self.many else None  # type: ignore

        # Convert to object instances
        target_class = obj._client._object_registry.get(self.target_type)
        if target_class is None:
            # Return raw data if target class not registered
            if self.many:
                return neighbors  # type: ignore
            return neighbors[0] if neighbors else None  # type: ignore

        objects = [
            target_class._from_resolved(n, obj._client) for n in neighbors
        ]

        if self.many:
            return objects  # type: ignore
        return objects[0] if objects else None  # type: ignore


# ========================================
# ObjectSet (Query Builder)
# ========================================


class ObjectSet(Generic[T]):
    """
    Query builder for Objects.

    Provides a fluent API for building queries:
        client.objects.Customer.where(Customer.status == "active").list()
    """

    def __init__(
        self,
        client: CosmosClient,
        object_type: type[T],
        object_type_key: str,
    ):
        self._client = client
        self._object_type = object_type
        self._object_type_key = object_type_key
        self._filters: list[PropertyComparison | CompositeFilter] = []
        self._search_query: str | None = None
        self._selected_fields: list[str] | None = None
        self._include: list[str] = []
        self._limit: int | None = None
        self._offset: int | None = None
        self._sort_by: str | None = None
        self._sort_order: Literal["asc", "desc"] | None = None
        # For search_around chaining
        self._traversal_steps: list[dict[str, Any]] = []
        # Filters to apply after traversal (added after search_around)
        self._post_traversal_filters: list[PropertyComparison | CompositeFilter] = []

    def where(self, *conditions: PropertyComparison | CompositeFilter) -> Self:
        """Add filter conditions."""
        new_set = self._copy()
        # If we have traversal steps, these filters apply after traversal
        if new_set._traversal_steps:
            new_set._post_traversal_filters.extend(conditions)
        else:
            new_set._filters.extend(conditions)
        return new_set

    def search(self, query: str) -> Self:
        """Add text search query."""
        new_set = self._copy()
        new_set._search_query = query
        return new_set

    def select(self, *fields: Property | str | list[str]) -> Self:
        """Select specific fields to return.

        Can be called as:
            .select('field1', 'field2')  # Multiple arguments
            .select(['field1', 'field2'])  # Single list argument
        """
        new_set = self._copy()
        field_names = []

        # Handle case where a single list is passed
        if len(fields) == 1 and isinstance(fields[0], list):
            fields = fields[0]

        for f in fields:
            if isinstance(f, Property):
                field_names.append(f._name)
            else:
                field_names.append(f)
        new_set._selected_fields = field_names
        return new_set

    def include(self, *links: str) -> Self:
        """Eager load related objects."""
        new_set = self._copy()
        new_set._include.extend(links)
        return new_set

    def search_around(self, link_name: str, direction: str = "outgoing") -> ObjectSet:
        """
        Traverse to related objects via a link.

        This enables multi-hop graph traversal:
            orders = client.objects.Order.where(Order.status == "completed")
            customers = orders.search_around("customer")
            addresses = customers.search_around("address")

        Args:
            link_name: Name of the link to traverse
            direction: "outgoing", "incoming", or "both"

        Returns:
            A new ObjectSet for traversing the relationship
        """
        new_set = self._copy()
        new_set._traversal_steps.append({
            "link_type": link_name,
            "direction": direction,
        })
        return new_set

    def order_by(self, field: Property | str, order: Literal["asc", "desc"] = "asc") -> Self:
        """Sort results."""
        new_set = self._copy()
        if isinstance(field, Property):
            new_set._sort_by = field._name
        else:
            new_set._sort_by = field
        new_set._sort_order = order
        return new_set

    def limit(self, n: int) -> Self:
        """Limit number of results."""
        new_set = self._copy()
        new_set._limit = n
        return new_set

    def offset(self, n: int) -> Self:
        """Skip first n results."""
        new_set = self._copy()
        new_set._offset = n
        return new_set

    def _copy(self) -> Self:
        """Create a copy of this ObjectSet."""
        new_set = ObjectSet(
            self._client,
            self._object_type,
            self._object_type_key,
        )
        new_set._filters = self._filters.copy()
        new_set._search_query = self._search_query
        new_set._selected_fields = (
            self._selected_fields.copy() if self._selected_fields else None
        )
        new_set._include = self._include.copy()
        new_set._limit = self._limit
        new_set._offset = self._offset
        new_set._sort_by = self._sort_by
        new_set._sort_order = self._sort_order
        new_set._traversal_steps = self._traversal_steps.copy()
        new_set._post_traversal_filters = self._post_traversal_filters.copy()
        return new_set  # type: ignore

    def _build_search_query(self) -> dict[str, Any]:
        """Build SearchQuery for API call."""
        query: dict[str, Any] = {}

        if self._search_query:
            query["query"] = self._search_query

        if self._filters:
            filters = self._flatten_filters(self._filters)
            query["filters"] = filters

        # When using limit or offset without explicit sort, add default sort by object_id
        # to ensure consistent ordering across queries (important for pagination)
        sort_by = self._sort_by
        sort_order = self._sort_order
        if (self._limit or self._offset) and not sort_by:
            sort_by = "object_id"
            sort_order = "asc"

        if sort_by:
            query["sort_by"] = sort_by
        if sort_order:
            query["sort_order"] = sort_order
        if self._limit:
            query["limit"] = self._limit
        if self._offset:
            query["offset"] = self._offset

        return query

    def _flatten_filters(
        self, conditions: list[PropertyComparison | CompositeFilter]
    ) -> list[dict[str, Any]]:
        """
        Flatten filters including CompositeFilter (AND/OR).

        Note: OR conditions are converted to 'in' operator when possible,
        otherwise they are expanded as separate filters (which results in AND behavior).
        For proper OR support, the server would need to support nested bool queries.
        """
        filters = []
        for condition in conditions:
            if isinstance(condition, PropertyComparison):
                filters.append({
                    "field": condition.field,
                    "op": condition.op,
                    "value": condition.value,
                })
            elif isinstance(condition, CompositeFilter):
                if condition.operator == "and":
                    # AND: just flatten all conditions
                    filters.extend(self._flatten_filters(condition.conditions))
                elif condition.operator == "or":
                    # OR: try to optimize to 'in' query if all conditions are on same field with 'eq'
                    or_filters = self._try_convert_or_to_in(condition.conditions)
                    if or_filters:
                        filters.extend(or_filters)
                    else:
                        # Fallback: flatten (Note: this results in AND behavior, not OR)
                        # Proper OR requires server-side bool query support
                        filters.extend(self._flatten_filters(condition.conditions))
        return filters

    def _try_convert_or_to_in(
        self, conditions: list[PropertyComparison | CompositeFilter]
    ) -> list[dict[str, Any]] | None:
        """
        Try to convert OR conditions to 'in' operator.

        This only works when:
        - All conditions are PropertyComparison (not nested CompositeFilter)
        - All conditions use 'eq' operator
        - All conditions are on the same field

        Returns None if conversion is not possible.
        """
        if not all(isinstance(c, PropertyComparison) for c in conditions):
            return None

        comparisons = [c for c in conditions if isinstance(c, PropertyComparison)]

        if not all(c.op == "eq" for c in comparisons):
            return None

        # Group by field
        fields = set(c.field for c in comparisons)

        if len(fields) == 1:
            # All same field - convert to 'in'
            field = comparisons[0].field
            values = [c.value for c in comparisons]
            return [{
                "field": field,
                "op": "in",
                "value": values,
            }]
        else:
            # Different fields - can't convert to simple 'in'
            # Return None to use fallback
            return None

    async def list(self) -> ObjectList[T]:
        """Execute query and return results as ObjectList."""
        from cosmos_sdk._internal.types import (
            EdgeDirection,
            SearchQuery,
            TraversalRequest,
            TraversalStep,
        )

        api_client = self._client._api_client

        # Handle traversal (search_around)
        if self._traversal_steps:
            return await self._execute_traversal()

        # Always use search endpoint - it properly supports:
        # - Scroll search for unlimited results when limit=0
        # - Server-side filtering, sorting, aggregations
        # - Consistent pagination behavior
        # The list endpoint has a default limit of 100 which is not what we want
        search_query = SearchQuery(**self._build_search_query())
        result = await api_client.search_objects(
            self._object_type_key,
            search_query,
        )
        objects = result.objects

        # Convert to ObjectList
        items = [
            self._object_type._from_resolved(obj, self._client) for obj in objects
        ]

        # Eager load included links
        if self._include:
            for item in items:
                await item.load_links(*self._include)

        return ObjectList(items)

    async def _execute_traversal(self) -> ObjectList[T]:
        """Execute graph traversal with search_around steps using FK relationships."""
        from cosmos_sdk._internal.types import SearchFilter, SearchQuery

        api_client = self._client._api_client

        # First, get starting objects (with any filters applied)
        if self._filters or self._search_query:
            search_query = SearchQuery(**self._build_search_query())
            start_result = await api_client.search_objects(
                self._object_type_key,
                search_query,
            )
            start_objects = start_result.objects
        else:
            start_result = await api_client.list_objects(
                self._object_type_key,
                limit=self._limit or 1000,
            )
            start_objects = start_result.objects

        if not start_objects:
            return ObjectList([])

        # Process each traversal step using FK relationships
        current_objects = start_objects
        current_type = self._object_type_key

        for step in self._traversal_steps:
            link_name = step["link_type"]
            direction = step["direction"]

            # Get LinkType info to find FK configuration
            # Try multiple approaches: by name, by key, by listing all
            link_info = None

            # 1. Try get_link_type_by_name API
            try:
                link_info = await api_client.get_link_type_by_name(
                    link_name,
                    source_type=current_type if direction == "outgoing" else None,
                    target_type=current_type if direction == "incoming" else None,
                )
            except Exception:
                pass

            # 2. Try with link_type_key directly
            if not link_info:
                try:
                    link_type = await api_client.get_link_type(link_name)
                    link_info = (link_type, False)
                except Exception:
                    pass

            # 3. Search by name in all link types
            if not link_info:
                try:
                    all_links = await api_client.list_link_types()
                    for lt in all_links.link_types:
                        # Match by name (case-insensitive) or key
                        if lt.name.lower() == link_name.lower() or lt.link_type_key == link_name:
                            # Check if direction matches
                            if direction == "outgoing" and lt.source_type == current_type:
                                link_info = (lt, False)
                                break
                            elif direction == "incoming" and lt.target_type == current_type:
                                link_info = (lt, True)
                                break
                            elif direction == "both":
                                is_reverse = lt.target_type == current_type
                                link_info = (lt, is_reverse)
                                break
                except Exception:
                    pass

            if not link_info:
                # Link not found, return empty
                return ObjectList([])

            link_type, is_reverse = link_info
            fk_config = link_type.fk_config

            if not fk_config:
                # No FK config, can't traverse without edges
                return ObjectList([])

            # FK config: source_field is on source_type, target_pk_field is on target_type
            # Example: Factory(source) has factory_id, Inventory(target) has factory_id

            # Determine traversal direction
            # outgoing: current_type is source, we want target
            # incoming: current_type is target, we want source
            is_outgoing = (direction == "outgoing" and not is_reverse) or (direction == "incoming" and is_reverse)

            if is_outgoing:
                # We are source, finding targets
                # Get source_field values from current objects, search target by target_pk_field
                target_type_key = link_type.target_type
                current_field = fk_config.source_field
                search_field = fk_config.target_pk_field
            else:
                # We are target, finding sources
                # Get target_pk_field values from current objects, search source by source_field
                target_type_key = link_type.source_type
                current_field = fk_config.target_pk_field
                search_field = fk_config.source_field

            # Get FK values from current objects
            fk_values = [
                obj.effective_state.get(current_field)
                for obj in current_objects
                if obj.effective_state.get(current_field) is not None
            ]

            if not fk_values:
                return ObjectList([])

            # Remove duplicates
            fk_values = list(set(fk_values))

            # Build filters list with FK constraint
            filters = [SearchFilter(field=search_field, op="in", value=fk_values)]

            # If this is the last traversal step and we have post-traversal filters, add them
            is_last_step = step == self._traversal_steps[-1]
            if is_last_step and self._post_traversal_filters:
                # Flatten and add post-traversal filters
                flat_filters = self._flatten_filters(self._post_traversal_filters)
                for f in flat_filters:
                    filters.append(SearchFilter(
                        field=f["field"],
                        op=f["op"],
                        value=f["value"],
                    ))

            # Search for target objects using FK values and filters
            search_query = SearchQuery(
                filters=filters,
                limit=self._limit or 1000,
            )

            target_result = await api_client.search_objects(
                target_type_key,
                search_query,
            )

            current_objects = target_result.objects
            current_type = target_type_key

            if not current_objects:
                return ObjectList([])

        # Convert to object instances
        items = [
            self._object_type._from_resolved(obj, self._client)
            for obj in current_objects
        ]

        # Eager load included links
        if self._include:
            for item in items:
                await item.load_links(*self._include)

        return ObjectList(items)

    async def get(self, object_id: str) -> T:
        """Get a single object by ID."""
        api_client = self._client._api_client
        resolved = await api_client.get_object(self._object_type_key, object_id)
        obj = self._object_type._from_resolved(resolved, self._client)

        # Eager load included links
        if self._include:
            await obj.load_links(*self._include)

        return obj

    async def first(self) -> T | None:
        """Get the first result or None."""
        result = await self.limit(1).list()
        return result[0] if result else None

    async def count(self) -> int:
        """Count matching objects."""
        from cosmos_sdk._internal.types import SearchQuery

        api_client = self._client._api_client
        search_query = SearchQuery(**self._build_search_query(), limit=0)
        result = await api_client.search_objects(
            self._object_type_key,
            search_query,
        )
        return result.total

    async def iterate(self, batch_size: int = 100) -> Iterator[T]:
        """Iterate through all results with automatic pagination."""
        offset = 0
        while True:
            result = await self.limit(batch_size).offset(offset).list()
            if not result:
                break
            for item in result:
                yield item
            if len(result) < batch_size:
                break
            offset += batch_size

    def to_dataframe(self) -> pl.DataFrame:
        """
        Execute query and return results as Polars DataFrame.

        Note: This is a synchronous convenience method.
        For async code, use `list()` then `to_dataframe()` on the result.
        """
        if not HAS_POLARS:
            raise ImportError("polars is required for to_dataframe(). Install with: pip install cosmos-sdk[polars]")

        import asyncio

        result = asyncio.get_event_loop().run_until_complete(self.list())
        df = result.to_dataframe()

        # Apply select if specified
        if self._selected_fields and not df.is_empty():
            # Only keep requested columns that exist in the dataframe
            available_cols = set(df.columns)
            cols_to_keep = [c for c in self._selected_fields if c in available_cols]
            if cols_to_keep:
                df = df.select(cols_to_keep)

        return df

    # Aggregation methods (server-side)
    async def sum(self, field: Property | str) -> float:
        """Sum values of a field (server-side aggregation)."""
        field_name = field._name if isinstance(field, Property) else field
        result = await self._aggregate([{"name": "result", "type": "sum", "field": field_name}])
        return result.get("result", 0) or 0

    async def avg(self, field: Property | str) -> float:
        """Average values of a field (server-side aggregation)."""
        field_name = field._name if isinstance(field, Property) else field
        result = await self._aggregate([{"name": "result", "type": "avg", "field": field_name}])
        return result.get("result", 0) or 0

    async def min(self, field: Property | str) -> Any:
        """Minimum value of a field (server-side aggregation)."""
        field_name = field._name if isinstance(field, Property) else field
        result = await self._aggregate([{"name": "result", "type": "min", "field": field_name}])
        return result.get("result")

    async def max(self, field: Property | str) -> Any:
        """Maximum value of a field (server-side aggregation)."""
        field_name = field._name if isinstance(field, Property) else field
        result = await self._aggregate([{"name": "result", "type": "max", "field": field_name}])
        return result.get("result")

    async def stats(self, field: Property | str) -> dict[str, Any]:
        """Get all stats (count, sum, avg, min, max) for a field (server-side aggregation)."""
        field_name = field._name if isinstance(field, Property) else field
        result = await self._aggregate([{"name": "result", "type": "stats", "field": field_name}])
        return result.get("result", {})

    async def _aggregate(self, metrics: list[dict[str, str]]) -> dict[str, Any]:
        """Execute server-side aggregation."""
        from cosmos_sdk._internal.types import (
            MetricRequest,
            ObjectAggregateRequest,
            SearchFilter,
        )

        api_client = self._client._api_client

        # Build filters from conditions
        filters = None
        if self._filters:
            filters = []
            for f in self._filters:
                if isinstance(f, PropertyComparison):
                    filters.append(SearchFilter(
                        field=f.field,
                        op=f.op,
                        value=f.value,
                    ))

        # Build metric requests
        metric_requests = [
            MetricRequest(name=m["name"], type=m["type"], field=m.get("field", ""))
            for m in metrics
        ]

        request = ObjectAggregateRequest(
            filters=filters,
            metrics=metric_requests,
        )

        result = await api_client.aggregate_objects(
            self._object_type_key,
            request,
        )

        return result.metrics or {}

    def group_by(self, *fields: Property | str) -> GroupedObjectSet[T]:
        """
        Group results by one or more fields.

        Example:
            results = await client.objects.Order \
                .where(Order.status == "completed") \
                .group_by(Order.customer_id) \
                .agg(count=Agg.count(), total=Agg.sum(Order.total)) \
                .list()
        """
        field_names = []
        for f in fields:
            if isinstance(f, Property):
                field_names.append(f._name)
            else:
                field_names.append(f)

        return GroupedObjectSet(self, field_names)


# ========================================
# Aggregation Helpers
# ========================================


class Agg:
    """
    Aggregation function definitions for use with group_by().

    Example:
        .agg(count=Agg.count(), total=Agg.sum(Order.total))
    """

    @staticmethod
    def count() -> dict[str, Any]:
        """Count aggregation."""
        return {"func": "count"}

    @staticmethod
    def sum(field: Property | str) -> dict[str, Any]:
        """Sum aggregation."""
        field_name = field._name if isinstance(field, Property) else field
        return {"func": "sum", "field": field_name}

    @staticmethod
    def avg(field: Property | str) -> dict[str, Any]:
        """Average aggregation."""
        field_name = field._name if isinstance(field, Property) else field
        return {"func": "avg", "field": field_name}

    @staticmethod
    def min(field: Property | str) -> dict[str, Any]:
        """Minimum aggregation."""
        field_name = field._name if isinstance(field, Property) else field
        return {"func": "min", "field": field_name}

    @staticmethod
    def max(field: Property | str) -> dict[str, Any]:
        """Maximum aggregation."""
        field_name = field._name if isinstance(field, Property) else field
        return {"func": "max", "field": field_name}


class GroupedObjectSet(Generic[T]):
    """
    Represents a grouped query result.

    Created by ObjectSet.group_by().
    """

    def __init__(self, object_set: ObjectSet[T], group_fields: list[str]):
        self._object_set = object_set
        self._group_fields = group_fields
        self._aggregations: dict[str, dict[str, Any]] = {}

    def agg(self, **aggregations: dict[str, Any]) -> GroupedObjectSet[T]:
        """
        Define aggregations to compute for each group.

        Args:
            **aggregations: Named aggregations using Agg helper

        Example:
            .agg(count=Agg.count(), total=Agg.sum(Order.total))
        """
        new_grouped = GroupedObjectSet(self._object_set, self._group_fields)
        new_grouped._aggregations = {**self._aggregations, **aggregations}
        return new_grouped

    async def list(self) -> list[dict[str, Any]]:
        """
        Execute grouped aggregation and return results (server-side).

        Returns list of dicts with group keys and aggregation values.
        """
        from cosmos_sdk._internal.types import (
            MetricRequest,
            ObjectAggregateRequest,
            SearchFilter,
        )

        api_client = self._object_set._client._api_client

        # Build filters from conditions
        filters = None
        if self._object_set._filters:
            filters = []
            for f in self._object_set._filters:
                if isinstance(f, PropertyComparison):
                    filters.append(SearchFilter(
                        field=f.field,
                        op=f.op,
                        value=f.value,
                    ))

        # Build metric requests from aggregations
        metric_requests = []
        for name, agg_def in self._aggregations.items():
            func = agg_def["func"]
            field = agg_def.get("field", "")

            # Map Agg helper functions to API types
            agg_type = func  # count, sum, avg, min, max map directly
            metric_requests.append(MetricRequest(
                name=name,
                type=agg_type,
                field=field,
            ))

        request = ObjectAggregateRequest(
            filters=filters,
            group_by=self._group_fields,
            metrics=metric_requests,
        )

        result = await api_client.aggregate_objects(
            self._object_set._object_type_key,
            request,
        )

        # Return buckets (grouped results)
        return result.buckets or []

    def to_dataframe(self) -> pl.DataFrame:
        """Execute and return results as Polars DataFrame."""
        if not HAS_POLARS:
            raise ImportError("polars is required for to_dataframe(). Install with: pip install cosmos-sdk[polars]")

        import asyncio

        results = asyncio.get_event_loop().run_until_complete(self.list())
        return pl.DataFrame(results)


# ========================================
# ObjectList (Results Container)
# ========================================


class ObjectList(Generic[T]):
    """
    List of Objects returned from a query.

    Supports iteration, indexing, and DataFrame conversion.
    """

    def __init__(self, items: list[T]):
        self._items = items

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def __getitem__(self, index: int) -> T:
        return self._items[index]

    def __bool__(self) -> bool:
        return len(self._items) > 0

    def to_dataframe(self) -> pl.DataFrame:
        """Convert to Polars DataFrame."""
        if not HAS_POLARS:
            raise ImportError("polars is required for to_dataframe(). Install with: pip install cosmos-sdk[polars]")

        if not self._items:
            return pl.DataFrame()

        rows = []
        for item in self._items:
            rows.append(item._data.copy())

        return pl.DataFrame(rows, infer_schema_length=None)

    def to_list(self) -> list[T]:
        """Return as plain Python list."""
        return self._items.copy()


# ========================================
# BaseObject
# ========================================


class BaseObject:
    """
    Base class for all Object types.

    Subclasses define properties and links as class attributes:

        class Customer(BaseObject):
            id = Property(type="string", primary_key=True)
            name = Property(type="string")
            orders = Link("Order", many=True)
    """

    # Class-level attributes set by code generator
    __object_type__: str = ""
    __object_type_key__: str = ""  # Deprecated, use __object_type__
    __primary_key__: str = "id"

    # ========================================
    # Class-level query methods (use singleton client)
    # ========================================

    @classmethod
    def _get_default_client(cls) -> CosmosClient:
        """Get the singleton CosmosClient instance."""
        from cosmos_sdk.client import CosmosClient
        return CosmosClient()

    @classmethod
    def _get_object_set(cls) -> "ObjectSet":
        """Get an ObjectSet for this type using the singleton client."""
        client = cls._get_default_client()
        object_type_key = getattr(cls, '__object_type__', '') or getattr(cls, '__object_type_key__', '') or cls.__name__.lower()
        return ObjectSet(client, cls, object_type_key)

    @classmethod
    def where(cls, *conditions: "PropertyComparison") -> "ObjectSet":
        """Filter objects by conditions."""
        return cls._get_object_set().where(*conditions)

    @classmethod
    def limit(cls, n: int) -> "ObjectSet":
        """Limit the number of results."""
        return cls._get_object_set().limit(n)

    @classmethod
    def offset(cls, n: int) -> "ObjectSet":
        """Skip the first n results."""
        return cls._get_object_set().offset(n)

    @classmethod
    def select(cls, *fields: str | list[str]) -> "ObjectSet":
        """Select specific fields.

        Can be called as:
            .select('field1', 'field2')  # Multiple arguments
            .select(['field1', 'field2'])  # Single list argument
        """
        return cls._get_object_set().select(*fields)

    @classmethod
    def order_by(cls, field: str, direction: str = "asc") -> "ObjectSet":
        """Order results by a field."""
        return cls._get_object_set().order_by(field, direction)

    @classmethod
    def search(cls, query: str) -> "ObjectSet":
        """Full-text search."""
        return cls._get_object_set().search(query)

    @classmethod
    async def list(cls) -> "ObjectList":
        """List all objects of this type."""
        return await cls._get_object_set().list()

    @classmethod
    async def first(cls) -> "Self | None":
        """Get the first object."""
        return await cls._get_object_set().first()

    @classmethod
    async def count(cls) -> int:
        """Count objects of this type."""
        return await cls._get_object_set().count()

    @classmethod
    async def get(cls, object_id: str) -> "Self | None":
        """Get an object by ID."""
        return await cls._get_object_set().get(object_id)

    @classmethod
    def to_dataframe(cls) -> "pl.DataFrame":
        """Convert all objects to a Polars DataFrame."""
        if not HAS_POLARS:
            raise ImportError("polars is required for to_dataframe(). Install with: pip install cosmos-sdk[polars]")
        return cls._get_object_set().to_dataframe()

    # ========================================
    # Instance methods
    # ========================================

    def __init__(self, **data: Any):
        self._data: dict[str, Any] = data
        self._client: CosmosClient | None = None

    @classmethod
    def _from_resolved(cls, resolved: ResolvedObject, client: CosmosClient) -> Self:
        """Create instance from ResolvedObject."""
        obj = cls(**resolved.effective_state)
        obj._data["_object_id"] = resolved.object_id
        obj._data["_version"] = resolved.version
        obj._client = client
        return obj

    @property
    def object_id(self) -> str:
        """Get the object's unique ID."""
        return self._data.get("_object_id") or self._data.get(self.__primary_key__)

    @property
    def version(self) -> int:
        """Get the object's version number."""
        return self._data.get("_version", 0)

    async def refresh(self) -> None:
        """Reload object data from the server."""
        if self._client is None:
            raise RuntimeError("Cannot refresh without a client")

        api_client = self._client._api_client
        resolved = await api_client.get_object(
            self.__object_type_key__,
            self.object_id,
        )
        self._data = resolved.effective_state
        self._data["_object_id"] = resolved.object_id
        self._data["_version"] = resolved.version

    async def update(self, **changes: Any) -> None:
        """
        Update object properties.

        Uses the Actions API to apply changes.
        """
        if self._client is None:
            raise RuntimeError("Cannot update without a client")

        from cosmos_sdk._internal.types import (
            AllowedOperation,
            ApplyActionInput,
            PropertyChange,
        )

        property_changes = [
            PropertyChange(property=key, op=AllowedOperation.SET, value=value)
            for key, value in changes.items()
        ]

        input_data = ApplyActionInput(
            object_type=self.__object_type_key__,
            object_ids=[self.object_id],
            changes=property_changes,
        )

        api_client = self._client._api_client
        await api_client.apply_action(input_data)

        # Update local state
        for key, value in changes.items():
            self._data[key] = value

    async def edges(self) -> list[Any]:
        """Get all edges connected to this object."""
        if self._client is None:
            raise RuntimeError("Cannot get edges without a client")

        api_client = self._client._api_client
        return await api_client.get_object_edges(
            self.__object_type_key__,
            self.object_id,
        )

    async def load_link(self, link_name: str) -> Any:
        """
        Explicitly load a link (async-safe).

        Use this method in async contexts instead of direct attribute access.

        Args:
            link_name: Name of the link to load

        Returns:
            Linked object(s)
        """
        if self._client is None:
            raise RuntimeError("Cannot load link without a client")

        # Check cache first
        cache_key = f"_link_{link_name}"
        cached = self._data.get(cache_key)
        if cached is not None:
            return cached

        # Find the Link descriptor
        link_descriptor = getattr(self.__class__, link_name, None)
        if link_descriptor is None or not isinstance(link_descriptor, Link):
            raise AttributeError(f"No link named '{link_name}' on {self.__class__.__name__}")

        # Load via the descriptor
        result = await link_descriptor._load_link(self)

        # Cache the result
        self._data[cache_key] = result
        return result

    async def load_links(self, *link_names: str) -> None:
        """
        Load multiple links at once.

        Args:
            *link_names: Names of links to load
        """
        for link_name in link_names:
            await self.load_link(link_name)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self.object_id}>"
