"""
Dataset SDK Pydantic 모델.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class FilterSpec(BaseModel):
    """행 필터 스펙 — 기존 API 필터 스펙과 동일."""

    field: str
    op: Literal["eq", "ne", "lt", "le", "gt", "ge", "in", "not_in", "is_null", "is_not_null"]
    value: Any = None


class RowsResult(BaseModel):
    """행 조작 API 결과."""

    rows_inserted: int = Field(0, alias="rowsInserted")
    rows_updated: int = Field(0, alias="rowsUpdated")
    rows_deleted: int = Field(0, alias="rowsDeleted")
    snapshot_id: str = Field("", alias="snapshotId")

    model_config = {"populate_by_name": True}


class PreviewResult(BaseModel):
    """데이터셋 preview 결과."""

    rows: list[dict[str, Any]] = Field(default_factory=list)
    total_count: int = Field(0, alias="totalCount")
    columns: list[str] = Field(default_factory=list)

    model_config = {"populate_by_name": True}
