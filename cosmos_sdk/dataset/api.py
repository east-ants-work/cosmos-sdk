"""
Dataset Service Low-level HTTP 클라이언트.

API 엔드포인트:
  GET  /api/v1/datasets/:key          — 메타데이터 조회
  POST /api/v1/datasets/:key/preview  — 데이터 조회
  POST /api/v1/datasets/:key/rows     — rows upsert/append/overwrite
  POST /api/v1/datasets/:key/rows/update — 조건부 업데이트
  POST /api/v1/datasets/:key/rows/delete — 조건부 삭제
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlencode

import httpx

from cosmos_sdk.dataset.auth import AuthManager
from cosmos_sdk.dataset.errors import AuthError, DatasetError, NotFoundError, PermissionError

logger = logging.getLogger(__name__)


class DatasetAPIClient:
    """
    Dataset Service API 저수준 클라이언트.

    인증은 AuthManager가 처리하며, 401 시 토큰 무효화 후 1회 재시도.
    """

    def __init__(self, auth: AuthManager, timeout: float = 60.0):
        self._auth = auth
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> DatasetAPIClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        body: Any | None = None,
        query: dict[str, Any] | None = None,
        retry_on_401: bool = True,
    ) -> Any:
        """
        HTTP 요청 실행.

        Returns:
            응답 JSON의 `data` 필드. data가 없으면 전체 응답 반환.

        Raises:
            AuthError: 인증 실패
            PermissionError: 권한 없음
            NotFoundError: 리소스 없음
            DatasetError: 기타 오류
        """
        client = await self._get_client()
        token = await self._auth.get_token(client)

        url = self._build_url(path, query)
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

        try:
            resp = await client.request(method, url, headers=headers, json=body)
        except httpx.TimeoutException as e:
            raise DatasetError(f"Request timeout: {method} {path}") from e
        except httpx.TransportError as e:
            raise DatasetError(f"Request failed: {e}") from e

        # 401: 토큰 무효화 후 1회 재시도
        if resp.status_code == 401 and retry_on_401:
            logger.debug("Dataset SDK: 401, invalidating token and retrying")
            self._auth.invalidate()
            return await self._request(method, path, body=body, query=query, retry_on_401=False)

        if not resp.is_success:
            self._raise_for_status(resp)

        if resp.status_code == 204:
            return None

        body_json = resp.json()
        if isinstance(body_json, dict) and "data" in body_json:
            return body_json["data"]
        return body_json

    def _build_url(self, path: str, query: dict[str, Any] | None) -> str:
        base = self._auth._base_url
        url = f"{base}{path}"
        if query:
            filtered = {k: str(v) for k, v in query.items() if v is not None}
            if filtered:
                url = f"{url}?{urlencode(filtered)}"
        return url

    def _raise_for_status(self, resp: httpx.Response) -> None:
        code = resp.status_code
        try:
            detail = resp.json().get("error", resp.text)
        except Exception:
            detail = resp.text

        if code == 401:
            raise AuthError(f"Unauthorized: {detail}", status_code=code)
        if code == 403:
            raise PermissionError(f"Forbidden: {detail}", status_code=code)
        if code == 404:
            raise NotFoundError(f"Not found: {detail}", status_code=code)
        raise DatasetError(
            f"Request failed ({code}): {detail}", status_code=code
        )

    # ----------------------------------------
    # Dataset 메타데이터
    # ----------------------------------------

    async def get_dataset(self, key: str) -> dict[str, Any]:
        """GET /api/v1/datasets/:key — 메타데이터 조회."""
        return await self._request("GET", f"/api/v1/datasets/{key}")

    async def get_dataset_by_name(self, graph_key: str, name: str) -> dict[str, Any] | None:
        """GET /api/v1/datasets?graphKey=...&name=... — name으로 단건 조회. 없으면 None."""
        try:
            return await self._request(
                "GET", "/api/v1/datasets", query={"graphKey": graph_key, "name": name}
            )
        except NotFoundError:
            return None

    # ----------------------------------------
    # Preview (데이터 조회)
    # ----------------------------------------

    async def preview(
        self,
        key: str,
        *,
        limit: int = 100,
        offset: int = 0,
        filters: list[dict[str, Any]] | None = None,
        select: list[str] | None = None,
    ) -> dict[str, Any]:
        """POST /api/v1/datasets/:key/preview — 데이터 조회."""
        body: dict[str, Any] = {"limit": limit, "offset": offset}
        if filters:
            body["filters"] = filters
        if select:
            body["select"] = select
        return await self._request("POST", f"/api/v1/datasets/{key}/preview", body=body)

    # ----------------------------------------
    # Rows 조작
    # ----------------------------------------

    async def upsert_rows(
        self,
        key: str,
        rows: list[dict[str, Any]],
        mode: str = "append",
    ) -> dict[str, Any]:
        """POST /api/v1/datasets/:key/rows — append / upsert / overwrite."""
        return await self._request(
            "POST",
            f"/api/v1/datasets/{key}/rows",
            body={"rows": rows, "mode": mode},
        )

    async def update_rows(
        self,
        key: str,
        filters: list[dict[str, Any]],
        updates: dict[str, Any],
    ) -> dict[str, Any]:
        """POST /api/v1/datasets/:key/rows/update — 조건부 업데이트."""
        return await self._request(
            "POST",
            f"/api/v1/datasets/{key}/rows/update",
            body={"filters": filters, "updates": updates},
        )

    async def delete_rows(
        self,
        key: str,
        filters: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """POST /api/v1/datasets/:key/rows/delete — 조건부 삭제. 빈 filters → truncate."""
        return await self._request(
            "POST",
            f"/api/v1/datasets/{key}/rows/delete",
            body={"filters": filters},
        )
