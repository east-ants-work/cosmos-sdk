"""
DatasetClient — Internal Dataset 외부 CRUD SDK.

기존 built-in function을 Connection String 기반으로 외부에서 사용할 수 있게 한다.

Built-in function 미러링:
    load_input(alias) / get_current_dataframe()  →  get_dataframe(key)
    overwrite_table(df)                          →  overwrite_table(key, df)
    append_to_table(df)                          →  append_to_table(key, df)
    update_rows(filters, updates)                →  update_rows(key, filters, updates)
    delete_rows(filters)                         →  delete_rows(key, filters)

사용 예시:
    from cosmos_sdk.dataset import DatasetClient

    client = DatasetClient("cosmos://admin%40cosmos.local:admin123%40@localhost:3001")
    df = client.get_dataframe_sync("my_dataset")
    client.overwrite_table_sync("my_dataset", transformed_df)
    client.close_sync()
"""

from __future__ import annotations

import asyncio
import logging
import math
from typing import TYPE_CHECKING, Any

from cosmos_sdk.dataset.api import DatasetAPIClient
from cosmos_sdk.dataset.auth import AuthManager
from cosmos_sdk.dataset.connection import CosmosConnection, get_connection_from_env, parse_connection_string
from cosmos_sdk.dataset.errors import BatchError, DatasetError, NotFoundError
from cosmos_sdk.dataset.types import FilterSpec, RowsResult

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)

_DEFAULT_BATCH_SIZE = 5_000


class DatasetClient:
    """
    Internal Dataset 외부 CRUD 클라이언트.

    Args:
        connection_string: "cosmos://email:password@host:port" 형식.
            생략하면 COSMOS_CONNECTION_STRING 환경변수 사용.
        base_url: connection_string 대신 명시적으로 지정할 base URL.
        email: 명시적 이메일.
        password: 명시적 패스워드.
        batch_size: 배치 크기 (기본 5,000행).
        timeout: HTTP 요청 타임아웃 (초, 기본 60).
    """

    def __init__(
        self,
        connection_string: str | None = None,
        *,
        graph_key: str | None = None,
        base_url: str | None = None,
        email: str | None = None,
        password: str | None = None,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        timeout: float = 60.0,
    ):
        conn = self._resolve_connection(connection_string, base_url, email, password)
        self._auth = AuthManager(conn.base_url, conn.email, conn.password)
        self._api = DatasetAPIClient(self._auth, timeout=timeout)
        self._batch_size = batch_size
        self._graph_key = graph_key

    @classmethod
    def _from_components(
        cls,
        auth: AuthManager,
        api: DatasetAPIClient,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        graph_key: str | None = None,
    ) -> DatasetClient:
        """이미 초기화된 auth/api 컴포넌트로 인스턴스 생성 (CosmosClient 내부 사용)."""
        instance = cls.__new__(cls)
        instance._auth = auth
        instance._api = api
        instance._batch_size = batch_size
        instance._graph_key = graph_key
        return instance

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_connection(
        connection_string: str | None,
        base_url: str | None,
        email: str | None,
        password: str | None,
    ) -> CosmosConnection:
        if connection_string:
            return parse_connection_string(connection_string)

        if base_url and email and password:
            return CosmosConnection(email=email, password=password, base_url=base_url)

        # 환경변수 fallback
        from_env = get_connection_from_env()
        if from_env:
            return from_env

        raise ValueError(
            "DatasetClient requires a connection_string, (base_url + email + password), "
            "or COSMOS_CONNECTION_STRING environment variable."
        )

    def _rows_to_dicts(self, df: pl.DataFrame) -> list[dict[str, Any]]:
        return df.to_dicts()

    def _dicts_to_df(self, rows: list[dict[str, Any]]) -> pl.DataFrame:
        import polars as pl

        if not rows:
            return pl.DataFrame()
        return pl.from_dicts(rows)

    async def _resolve_key(self, key_or_name: str) -> str:
        """
        dataset key 또는 name을 받아 key를 반환.

        - 'dataset_'로 시작하면 key로 간주 → 그대로 반환.
        - 그 외 → name으로 간주하여 graph_key 기반 검색.
          graph_key가 설정되지 않은 경우 DatasetError 발생.
        """
        if key_or_name.startswith("dataset_"):
            return key_or_name

        if not self._graph_key:
            raise DatasetError(
                f"Cannot resolve dataset name '{key_or_name}' without graph_key. "
                "Pass graph_key to DatasetClient or CosmosClient(graph=...)."
            )

        result = await self._api.get_dataset_by_name(self._graph_key, key_or_name)
        if result is None:
            raise NotFoundError(
                f"Dataset with name '{key_or_name}' not found in graph '{self._graph_key}'."
            )
        key = result.get("key")
        if not key:
            raise DatasetError(f"Dataset name '{key_or_name}' resolved but has no key field.")
        return key

    def _filter_specs_to_dicts(
        self, filters: list[FilterSpec | dict[str, Any]] | None
    ) -> list[dict[str, Any]]:
        if not filters:
            return []
        result = []
        for f in filters:
            if isinstance(f, FilterSpec):
                result.append(f.model_dump())
            else:
                result.append(f)
        return result

    # ------------------------------------------------------------------
    # Async public API
    # ------------------------------------------------------------------

    async def get_dataframe(
        self,
        key: str,
        *,
        filters: list[FilterSpec | dict[str, Any]] | None = None,
        select: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        데이터셋을 Polars DataFrame으로 읽기.

        key는 dataset key('dataset_...')나 name 모두 허용.
        페이지네이션을 통해 전체 데이터를 로드한다.
        """
        import polars as pl

        key = await self._resolve_key(key)
        filter_dicts = self._filter_specs_to_dicts(filters)
        all_rows: list[dict[str, Any]] = []
        offset = 0
        batch = self._batch_size

        while True:
            result = await self._api.preview(
                key,
                limit=batch,
                offset=offset,
                filters=filter_dicts or None,
                select=select,
            )
            rows: list[dict[str, Any]] = result.get("rows", []) if isinstance(result, dict) else []
            all_rows.extend(rows)

            if len(rows) < batch:
                break
            offset += batch

        if not all_rows:
            return pl.DataFrame()
        return pl.from_dicts(all_rows)

    async def overwrite_table(self, key: str, df: pl.DataFrame) -> RowsResult:
        """
        데이터셋 전체를 덮어쓰기 (truncate + append).

        key는 dataset key('dataset_...')나 name 모두 허용.
        첫 배치에서 mode="overwrite"를 사용하고, 이후 배치는 mode="append".
        """
        key = await self._resolve_key(key)
        rows = self._rows_to_dicts(df)
        if not rows:
            # 빈 DataFrame → delete_rows(빈 필터) = truncate
            result = await self._api.delete_rows(key, filters=[])
            return RowsResult.model_validate(result or {})

        total = len(rows)
        batches = math.ceil(total / self._batch_size)

        accumulated = RowsResult()
        for i in range(batches):
            chunk = rows[i * self._batch_size : (i + 1) * self._batch_size]
            mode = "overwrite" if i == 0 else "append"
            try:
                result = await self._api.upsert_rows(key, chunk, mode=mode)
                batch_result = RowsResult.model_validate(result or {})
                accumulated.rows_inserted += batch_result.rows_inserted
                accumulated.rows_updated += batch_result.rows_updated
                accumulated.rows_deleted += batch_result.rows_deleted
                accumulated.snapshot_id = batch_result.snapshot_id or accumulated.snapshot_id
            except DatasetError as e:
                raise BatchError(
                    str(e),
                    rows_completed=i * self._batch_size,
                    total_rows=total,
                    status_code=e.status_code,
                ) from e

        return accumulated

    async def append_to_table(self, key: str, df: pl.DataFrame) -> RowsResult:
        """데이터셋에 행 추가 (append). key는 dataset key나 name 모두 허용."""
        key = await self._resolve_key(key)
        rows = self._rows_to_dicts(df)
        if not rows:
            return RowsResult()

        total = len(rows)
        batches = math.ceil(total / self._batch_size)

        accumulated = RowsResult()
        for i in range(batches):
            chunk = rows[i * self._batch_size : (i + 1) * self._batch_size]
            try:
                result = await self._api.upsert_rows(key, chunk, mode="append")
                batch_result = RowsResult.model_validate(result or {})
                accumulated.rows_inserted += batch_result.rows_inserted
                accumulated.rows_updated += batch_result.rows_updated
                accumulated.rows_deleted += batch_result.rows_deleted
                accumulated.snapshot_id = batch_result.snapshot_id or accumulated.snapshot_id
            except DatasetError as e:
                raise BatchError(
                    str(e),
                    rows_completed=i * self._batch_size,
                    total_rows=total,
                    status_code=e.status_code,
                ) from e

        return accumulated

    async def update_rows(
        self,
        key: str,
        filters: list[FilterSpec | dict[str, Any]],
        updates: dict[str, Any],
    ) -> RowsResult:
        """조건부 행 업데이트. key는 dataset key나 name 모두 허용."""
        key = await self._resolve_key(key)
        filter_dicts = self._filter_specs_to_dicts(filters)
        result = await self._api.update_rows(key, filter_dicts, updates)
        return RowsResult.model_validate(result or {})

    async def delete_rows(
        self,
        key: str,
        filters: list[FilterSpec | dict[str, Any]] | None = None,
    ) -> RowsResult:
        """
        조건부 행 삭제. key는 dataset key나 name 모두 허용.

        filters가 없거나 빈 리스트이면 전체 삭제 (truncate).
        """
        key = await self._resolve_key(key)
        filter_dicts = self._filter_specs_to_dicts(filters)
        result = await self._api.delete_rows(key, filter_dicts)
        return RowsResult.model_validate(result or {})

    async def close(self) -> None:
        """HTTP 클라이언트 종료."""
        await self._api.close()

    async def __aenter__(self) -> DatasetClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Sync wrappers (asyncio.run 기반)
    # ------------------------------------------------------------------

    def get_dataframe_sync(
        self,
        key: str,
        *,
        filters: list[FilterSpec | dict[str, Any]] | None = None,
        select: list[str] | None = None,
    ) -> pl.DataFrame:
        """get_dataframe의 동기 래퍼."""
        return asyncio.run(self.get_dataframe(key, filters=filters, select=select))

    def overwrite_table_sync(self, key: str, df: pl.DataFrame) -> RowsResult:
        """overwrite_table의 동기 래퍼."""
        return asyncio.run(self.overwrite_table(key, df))

    def append_to_table_sync(self, key: str, df: pl.DataFrame) -> RowsResult:
        """append_to_table의 동기 래퍼."""
        return asyncio.run(self.append_to_table(key, df))

    def update_rows_sync(
        self,
        key: str,
        filters: list[FilterSpec | dict[str, Any]],
        updates: dict[str, Any],
    ) -> RowsResult:
        """update_rows의 동기 래퍼."""
        return asyncio.run(self.update_rows(key, filters, updates))

    def delete_rows_sync(
        self,
        key: str,
        filters: list[FilterSpec | dict[str, Any]] | None = None,
    ) -> RowsResult:
        """delete_rows의 동기 래퍼."""
        return asyncio.run(self.delete_rows(key, filters))

    def close_sync(self) -> None:
        """close의 동기 래퍼."""
        asyncio.run(self.close())
