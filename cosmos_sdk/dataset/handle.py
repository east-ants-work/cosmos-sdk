"""
Dataset 핸들 — DatasetClient 없이 직접 Dataset에 접근.

사용 예시:
    from cosmos_sdk import Dataset

    orders = Dataset("orders")
    df = await orders.to_dataframe()
    await orders.overwrite(new_df)
    await orders.append(new_rows)
    await orders.update_rows(filters=[...], updates={...})
    await orders.delete_rows(filters=[...])
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Any

from cosmos_sdk.dataset.api import DatasetAPIClient
from cosmos_sdk.dataset.auth import TokenAuthManager
from cosmos_sdk.dataset.client import DatasetClient
from cosmos_sdk.dataset.types import FilterSpec, RowsResult

if TYPE_CHECKING:
    import polars as pl


class Dataset:
    """
    DatasetClient 없이 직접 Dataset에 접근하는 핸들 클래스.

    환경변수에서 자동으로 인증 정보를 읽어 싱글톤 DatasetClient를 생성한다.

    Args:
        key_or_name: dataset key ('dataset_...' 형태) 또는 dataset name.
    """

    _shared_client: DatasetClient | None = None

    def __init__(self, key_or_name: str):
        self._key_or_name = key_or_name

    @classmethod
    def _get_shared_client(cls) -> DatasetClient:
        """환경에 맞는 DatasetClient를 자동 생성 (싱글톤)."""
        if cls._shared_client is not None:
            return cls._shared_client

        # 1) Token 모드 (Action/Analytics 실행 환경)
        token = os.environ.get("AUTH_TOKEN") or os.environ.get("COSMOS_AUTH_TOKEN")
        if token:
            ds_url = os.environ.get("DATASET_SERVICE_URL") or os.environ.get(
                "COSMOS_API_URL", "http://localhost:8009"
            )
            graph_key = os.environ.get("GRAPH_KEY") or os.environ.get("COSMOS_GRAPH_KEY")
            auth = TokenAuthManager(token, ds_url)
            api = DatasetAPIClient(auth, timeout=60.0)
            cls._shared_client = DatasetClient._from_components(auth, api, graph_key=graph_key)
            return cls._shared_client

        # 2) Connection string 모드 (standalone)
        graph_key = os.environ.get("COSMOS_GRAPH_KEY")
        cls._shared_client = DatasetClient(graph_key=graph_key)
        return cls._shared_client

    @classmethod
    def _reset_shared_client(cls) -> None:
        """싱글톤 클라이언트를 리셋 (테스트용)."""
        cls._shared_client = None

    # ------------------------------------------------------------------
    # Async API
    # ------------------------------------------------------------------

    async def to_dataframe(
        self,
        *,
        filters: list[FilterSpec | dict[str, Any]] | None = None,
        select: list[str] | None = None,
    ) -> pl.DataFrame:
        """Dataset을 Polars DataFrame으로 읽기."""
        client = self._get_shared_client()
        return await client.get_dataframe(self._key_or_name, filters=filters, select=select)

    async def overwrite(self, df: pl.DataFrame) -> RowsResult:
        """Dataset 전체를 덮어쓰기 (truncate + append)."""
        client = self._get_shared_client()
        return await client.overwrite_table(self._key_or_name, df)

    async def append(self, df: pl.DataFrame) -> RowsResult:
        """Dataset에 행 추가."""
        client = self._get_shared_client()
        return await client.append_to_table(self._key_or_name, df)

    async def update_rows(
        self,
        filters: list[FilterSpec | dict[str, Any]],
        updates: dict[str, Any],
    ) -> RowsResult:
        """조건부 행 업데이트."""
        client = self._get_shared_client()
        return await client.update_rows(self._key_or_name, filters, updates)

    async def delete_rows(
        self,
        filters: list[FilterSpec | dict[str, Any]] | None = None,
    ) -> RowsResult:
        """조건부 행 삭제. filters가 없으면 전체 삭제."""
        client = self._get_shared_client()
        return await client.delete_rows(self._key_or_name, filters)

    # ------------------------------------------------------------------
    # Sync wrappers
    # ------------------------------------------------------------------

    def to_dataframe_sync(
        self,
        *,
        filters: list[FilterSpec | dict[str, Any]] | None = None,
        select: list[str] | None = None,
    ) -> pl.DataFrame:
        """to_dataframe의 동기 래퍼."""
        return asyncio.run(self.to_dataframe(filters=filters, select=select))

    def overwrite_sync(self, df: pl.DataFrame) -> RowsResult:
        """overwrite의 동기 래퍼."""
        return asyncio.run(self.overwrite(df))

    def append_sync(self, df: pl.DataFrame) -> RowsResult:
        """append의 동기 래퍼."""
        return asyncio.run(self.append(df))

    def update_rows_sync(
        self,
        filters: list[FilterSpec | dict[str, Any]],
        updates: dict[str, Any],
    ) -> RowsResult:
        """update_rows의 동기 래퍼."""
        return asyncio.run(self.update_rows(filters, updates))

    def delete_rows_sync(
        self,
        filters: list[FilterSpec | dict[str, Any]] | None = None,
    ) -> RowsResult:
        """delete_rows의 동기 래퍼."""
        return asyncio.run(self.delete_rows(filters))

    def __repr__(self) -> str:
        return f"Dataset({self._key_or_name!r})"
