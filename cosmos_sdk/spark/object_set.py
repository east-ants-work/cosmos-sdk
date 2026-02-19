"""
SparkObjectSet — Query builder that returns PySpark DataFrames.

Supports three backends:
- api: Cosmos API HTTP endpoint -> JSON -> spark.createDataFrame()
- objectdb: ObjectDB HTTP endpoint -> JSON -> spark.createDataFrame()
- iceberg: spark.read.format("iceberg").load() (native Spark)
"""

from __future__ import annotations

import json
import urllib.request
from typing import TYPE_CHECKING, Any

from cosmos_sdk.base import CompositeFilter, PropertyComparison

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


# Operator mapping for Spark filter expressions
_SPARK_OP_MAP = {
    "eq": "==",
    "ne": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
}


class SparkObjectSet:
    """
    Query builder for Objects that returns PySpark DataFrames.

    Immutable: each chaining method returns a new SparkObjectSet.
    """

    def __init__(
        self,
        spark: SparkSession,
        object_type: str,
        object_type_key: str,
        backend: str,
        base_url: str | None = None,
        token: str | None = None,
        object_metadata: dict[str, Any] | None = None,
        security_rules: dict[str, Any] | None = None,
        timeout: float = 30.0,
    ):
        self._spark = spark
        self._object_type = object_type
        self._object_type_key = object_type_key
        self._backend = backend
        self._base_url = base_url
        self._token = token
        self._object_metadata = object_metadata or {}
        self._security_rules = security_rules or {}
        self._timeout = timeout

        # Query parameters (immutable copy on each chain)
        self._filters: list[PropertyComparison | CompositeFilter] = []
        self._limit_val: int | None = None
        self._offset_val: int | None = None
        self._selected_fields: list[str] | None = None

    def _copy(self) -> SparkObjectSet:
        """Create an immutable copy for chaining."""
        new = SparkObjectSet(
            spark=self._spark,
            object_type=self._object_type,
            object_type_key=self._object_type_key,
            backend=self._backend,
            base_url=self._base_url,
            token=self._token,
            object_metadata=self._object_metadata,
            security_rules=self._security_rules,
            timeout=self._timeout,
        )
        new._filters = self._filters.copy()
        new._limit_val = self._limit_val
        new._offset_val = self._offset_val
        new._selected_fields = self._selected_fields.copy() if self._selected_fields else None
        return new

    def where(self, *conditions: PropertyComparison | CompositeFilter) -> SparkObjectSet:
        """Add filter conditions."""
        new = self._copy()
        new._filters.extend(conditions)
        return new

    def limit(self, n: int) -> SparkObjectSet:
        """Limit number of results."""
        new = self._copy()
        new._limit_val = n
        return new

    def offset(self, n: int) -> SparkObjectSet:
        """Skip first n results."""
        new = self._copy()
        new._offset_val = n
        return new

    def select(self, *fields: str) -> SparkObjectSet:
        """Select specific fields to return."""
        new = self._copy()
        new._selected_fields = list(fields)
        return new

    def to_dataframe(self) -> DataFrame:
        """Execute the query and return a PySpark DataFrame."""
        if self._backend == "iceberg":
            return self._load_iceberg()
        elif self._backend == "api":
            return self._load_api()
        elif self._backend == "objectdb":
            return self._load_objectdb()
        else:
            raise ValueError(f"Unknown backend: {self._backend}")

    # ------------------------------------------------------------------
    # Backend: Iceberg (native Spark read)
    # ------------------------------------------------------------------

    def _load_iceberg(self) -> DataFrame:
        """Load data from Iceberg table via Nessie catalog."""
        meta = self._object_metadata.get(self._object_type)
        if meta is None:
            available = list(self._object_metadata.keys())
            raise ValueError(
                f"Object '{self._object_type}' not found in metadata. "
                f"Available: {available}"
            )

        iceberg_table = meta.get("icebergTable")
        if not iceberg_table:
            raise ValueError(f"Object '{self._object_type}' has no icebergTable defined")

        df = self._spark.read.format("iceberg").load(f"nessie.{iceberg_table}")

        # Apply security rules
        df = self._apply_security_rules(df)

        # Apply filters
        df = self._apply_spark_filters(df)

        # Apply offset then limit
        if self._offset_val:
            # Spark doesn't have native offset; use monotonically_increasing_id workaround
            from pyspark.sql import functions as F

            df = df.withColumn("_row_num", F.monotonically_increasing_id())
            df = df.filter(F.col("_row_num") >= self._offset_val).drop("_row_num")

        if self._limit_val:
            df = df.limit(self._limit_val)

        # Apply select
        if self._selected_fields:
            existing = set(df.columns)
            cols = [c for c in self._selected_fields if c in existing]
            if cols:
                df = df.select(*cols)

        return df

    def _apply_security_rules(self, df: DataFrame) -> DataFrame:
        """Apply row/column security rules from config."""
        if not self._security_rules.get("hasAnyRestrictions"):
            return df

        rules_by_name = self._security_rules.get("byObjectName", {})
        rules = rules_by_name.get(self._object_type, {})

        # Row filter
        row_filter = rules.get("rowSecurityFilter")
        if row_filter:
            df = df.filter(row_filter)

        # Column filter
        accessible_fields = rules.get("accessibleFields", [])
        if accessible_fields:
            existing = set(df.columns)
            cols = [c for c in accessible_fields if c in existing]
            if cols:
                df = df.select(*cols)

        return df

    def _apply_spark_filters(self, df: DataFrame) -> DataFrame:
        """Apply PropertyComparison filters as Spark .filter() calls."""
        from pyspark.sql import functions as F

        for condition in self._filters:
            df = self._apply_single_filter(df, condition, F)
        return df

    def _apply_single_filter(self, df: DataFrame, condition: Any, F: Any) -> DataFrame:
        """Apply a single filter condition to a Spark DataFrame."""
        if isinstance(condition, PropertyComparison):
            col = F.col(condition.field)
            op = condition.op
            val = condition.value

            if op in _SPARK_OP_MAP:
                spark_op = _SPARK_OP_MAP[op]
                if spark_op == "==":
                    df = df.filter(col == val)
                elif spark_op == "!=":
                    df = df.filter(col != val)
                elif spark_op == ">":
                    df = df.filter(col > val)
                elif spark_op == ">=":
                    df = df.filter(col >= val)
                elif spark_op == "<":
                    df = df.filter(col < val)
                elif spark_op == "<=":
                    df = df.filter(col <= val)
            elif op == "in":
                df = df.filter(col.isin(val))
            elif op == "like":
                df = df.filter(col.like(val))
            elif op == "exists":
                if val:
                    df = df.filter(col.isNotNull())
                else:
                    df = df.filter(col.isNull())

        elif isinstance(condition, CompositeFilter):
            if condition.operator == "and":
                for sub in condition.conditions:
                    df = self._apply_single_filter(df, sub, F)
            elif condition.operator == "or":
                # Build OR expression
                or_expr = None
                for sub in condition.conditions:
                    sub_expr = self._build_filter_expr(sub, F)
                    if sub_expr is not None:
                        or_expr = sub_expr if or_expr is None else (or_expr | sub_expr)
                if or_expr is not None:
                    df = df.filter(or_expr)

        return df

    def _build_filter_expr(self, condition: Any, F: Any) -> Any:
        """Build a Spark Column expression from a filter condition."""
        if isinstance(condition, PropertyComparison):
            col = F.col(condition.field)
            op = condition.op
            val = condition.value

            if op == "eq":
                return col == val
            elif op == "ne":
                return col != val
            elif op == "gt":
                return col > val
            elif op == "gte":
                return col >= val
            elif op == "lt":
                return col < val
            elif op == "lte":
                return col <= val
            elif op == "in":
                return col.isin(val)
            elif op == "like":
                return col.like(val)
            elif op == "exists":
                return col.isNotNull() if val else col.isNull()

        elif isinstance(condition, CompositeFilter):
            if condition.operator == "and":
                expr = None
                for sub in condition.conditions:
                    sub_expr = self._build_filter_expr(sub, F)
                    if sub_expr is not None:
                        expr = sub_expr if expr is None else (expr & sub_expr)
                return expr
            elif condition.operator == "or":
                expr = None
                for sub in condition.conditions:
                    sub_expr = self._build_filter_expr(sub, F)
                    if sub_expr is not None:
                        expr = sub_expr if expr is None else (expr | sub_expr)
                return expr

        return None

    # ------------------------------------------------------------------
    # Backend: API (Cosmos API HTTP)
    # ------------------------------------------------------------------

    def _load_api(self) -> DataFrame:
        """Load data from Cosmos API endpoint."""
        if not self._base_url:
            raise ValueError("base_url is required for 'api' backend")

        # Build request for /api/objects/{type}/preview
        url = f"{self._base_url}/api/objects/{self._object_type_key}/preview"
        params = self._build_http_params()
        if params:
            query_string = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query_string}"

        data = self._http_get(url)
        return self._json_to_dataframe(data)

    def _load_objectdb(self) -> DataFrame:
        """Load data from ObjectDB endpoint."""
        if not self._base_url:
            raise ValueError("base_url is required for 'objectdb' backend")

        # Build search request for ObjectDB
        url = f"{self._base_url}/api/v1/objects/{self._object_type_key}/search"
        body = self._build_search_body()
        data = self._http_post(url, body)

        objects = data.get("objects", [])
        rows = []
        for obj in objects:
            row = obj.get("effective_state", obj.get("effectiveState", {}))
            row["_object_id"] = obj.get("object_id", obj.get("objectId"))
            rows.append(row)

        return self._json_to_dataframe(rows)

    # ------------------------------------------------------------------
    # HTTP helpers (stdlib only — no httpx needed in Spark env)
    # ------------------------------------------------------------------

    def _build_http_params(self) -> dict[str, str]:
        """Build query parameters for API requests."""
        params: dict[str, str] = {}
        if self._limit_val:
            params["limit"] = str(self._limit_val)
        if self._offset_val:
            params["offset"] = str(self._offset_val)
        return params

    def _build_search_body(self) -> dict[str, Any]:
        """Build search request body for ObjectDB."""
        body: dict[str, Any] = {}

        if self._filters:
            body["filters"] = self._flatten_filters(self._filters)

        if self._limit_val:
            body["limit"] = self._limit_val
        if self._offset_val:
            body["offset"] = self._offset_val

        return body

    def _flatten_filters(
        self, conditions: list[PropertyComparison | CompositeFilter]
    ) -> list[dict[str, Any]]:
        """Flatten filter conditions to API format."""
        filters = []
        for cond in conditions:
            if isinstance(cond, PropertyComparison):
                filters.append({
                    "field": cond.field,
                    "op": cond.op,
                    "value": cond.value,
                })
            elif isinstance(cond, CompositeFilter):
                if cond.operator == "and":
                    filters.extend(self._flatten_filters(cond.conditions))
                # OR is not fully supported in server-side; flatten as AND fallback
                else:
                    filters.extend(self._flatten_filters(cond.conditions))
        return filters

    def _http_get(self, url: str) -> Any:
        """Perform HTTP GET using stdlib urllib (no external dependencies)."""
        req = urllib.request.Request(url)
        if self._token:
            req.add_header("Authorization", f"Bearer {self._token}")
        req.add_header("Accept", "application/json")

        with urllib.request.urlopen(req, timeout=self._timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _http_post(self, url: str, body: dict[str, Any]) -> Any:
        """Perform HTTP POST using stdlib urllib."""
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        if self._token:
            req.add_header("Authorization", f"Bearer {self._token}")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")

        with urllib.request.urlopen(req, timeout=self._timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _json_to_dataframe(self, data: Any) -> DataFrame:
        """Convert JSON response to PySpark DataFrame."""
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            # Handle paginated responses
            rows = data.get("objects", data.get("rows", data.get("data", [])))
            if isinstance(rows, list) and rows and "effective_state" in rows[0]:
                rows = [
                    {**obj.get("effective_state", {}), "_object_id": obj.get("object_id")}
                    for obj in rows
                ]
        else:
            rows = []

        if not rows:
            return self._spark.createDataFrame([], schema="dummy: string").limit(0)

        df = self._spark.createDataFrame(rows)

        # Apply select
        if self._selected_fields:
            existing = set(df.columns)
            cols = [c for c in self._selected_fields if c in existing]
            if cols:
                df = df.select(*cols)

        return df
