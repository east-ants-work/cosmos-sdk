"""
Connection string 파서.

포맷: cosmos://email:password@host:port
특수문자(@, # 등)는 URL 인코딩 필요.

예시:
    cosmos://admin%40cosmos.local:admin123%40@localhost:3001
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import unquote, urlparse


@dataclass
class CosmosConnection:
    email: str
    password: str
    base_url: str  # e.g. "http://localhost:3001"


def parse_connection_string(conn_str: str) -> CosmosConnection:
    """
    Connection string을 파싱하여 CosmosConnection 반환.

    Args:
        conn_str: cosmos://email:password@host:port 형식

    Returns:
        CosmosConnection

    Raises:
        ValueError: 잘못된 connection string
    """
    parsed = urlparse(conn_str)

    if parsed.scheme != "cosmos":
        raise ValueError(
            f"Invalid connection string scheme '{parsed.scheme}'. Expected 'cosmos://'."
        )

    if not parsed.hostname:
        raise ValueError("Connection string must include a host.")

    email = unquote(parsed.username or "")
    password = unquote(parsed.password or "")

    if not email or not password:
        raise ValueError("Connection string must include email and password.")

    port = parsed.port
    host = parsed.hostname
    base_url = f"http://{host}:{port}" if port else f"http://{host}"

    return CosmosConnection(email=email, password=password, base_url=base_url)


def get_connection_from_env() -> CosmosConnection | None:
    """환경변수 COSMOS_CONNECTION_STRING에서 connection string 읽기."""
    conn_str = os.environ.get("COSMOS_CONNECTION_STRING")
    if not conn_str:
        return None
    return parse_connection_string(conn_str)
