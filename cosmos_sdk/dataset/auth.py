"""
JWT 인증 관리자.

- Lazy login: 첫 API 호출 시 자동 로그인
- 만료 60초 전 자동 refresh
- Refresh 실패 시 재로그인 fallback
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

from cosmos_sdk.dataset.errors import AuthError

logger = logging.getLogger(__name__)


class AuthManager:
    """JWT 토큰 수명 주기 관리."""

    def __init__(self, base_url: str, email: str, password: str):
        self._base_url = base_url.rstrip("/")
        self._email = email
        self._password = password

        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._expires_at: float = 0.0  # Unix timestamp

        self._lock = asyncio.Lock()

    async def get_token(self, client: httpx.AsyncClient) -> str:
        """유효한 access token 반환. 필요 시 자동 갱신."""
        async with self._lock:
            now = time.time()

            # 아직 로그인 안 됨
            if self._access_token is None:
                await self._login(client)
                return self._access_token  # type: ignore[return-value]

            # 60초 전 refresh
            if now >= self._expires_at - 60:
                await self._refresh_or_relogin(client)

            return self._access_token  # type: ignore[return-value]

    def invalidate(self) -> None:
        """토큰 무효화 (401 응답 시 호출)."""
        self._access_token = None
        self._refresh_token = None
        self._expires_at = 0.0

    async def _login(self, client: httpx.AsyncClient) -> None:
        """POST /api/v1/auth/login으로 로그인."""
        logger.debug("Dataset SDK: logging in as %s", self._email)
        try:
            resp = await client.post(
                f"{self._base_url}/api/v1/auth/login",
                json={"email": self._email, "password": self._password},
            )
        except httpx.TransportError as e:
            raise AuthError(f"Login request failed: {e}") from e

        if resp.status_code == 401:
            raise AuthError("Login failed: invalid email or password.")
        if not resp.is_success:
            raise AuthError(
                f"Login failed: {resp.status_code} {resp.text}", status_code=resp.status_code
            )

        data = self._extract_data(resp)
        self._store_tokens(data)

    async def _refresh_or_relogin(self, client: httpx.AsyncClient) -> None:
        """토큰 refresh 시도. 실패 시 재로그인."""
        if self._refresh_token:
            try:
                await self._do_refresh(client)
                return
            except AuthError:
                logger.debug("Dataset SDK: refresh failed, re-logging in")

        await self._login(client)

    async def _do_refresh(self, client: httpx.AsyncClient) -> None:
        """POST /api/v1/auth/refresh로 토큰 갱신."""
        resp = await client.post(
            f"{self._base_url}/api/v1/auth/refresh",
            json={"refreshToken": self._refresh_token},
        )
        if not resp.is_success:
            raise AuthError(
                f"Token refresh failed: {resp.status_code}", status_code=resp.status_code
            )

        data = self._extract_data(resp)
        self._store_tokens(data)

    def _extract_data(self, resp: httpx.Response) -> dict[str, Any]:
        body = resp.json()
        if isinstance(body, dict) and "data" in body:
            return body["data"]
        return body

    def _store_tokens(self, data: dict[str, Any]) -> None:
        self._access_token = data["access_token"]
        self._refresh_token = data.get("refresh_token")
        expires_in: int = data.get("expires_in", 3600)
        self._expires_at = time.time() + expires_in
