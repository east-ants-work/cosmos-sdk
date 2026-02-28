"""
cosmos-codegen CLI

조직(Graph)의 Object/Link 타입 정의를 Cosmos API에서 가져와
로컬 Python 클래스 파일을 생성한다.

Usage:
    # Connection string
    cosmos-codegen generate \\
        --connection "cosmos://admin%40cosmos.local:admin123%40@localhost:3001" \\
        --graph my_graph \\
        --output ./cosmos_objects

    # 명시적 파라미터
    cosmos-codegen generate \\
        --base-url http://localhost:3001 \\
        --email admin@cosmos.local \\
        --password "admin123@" \\
        --graph my_graph \\
        --output ./cosmos_objects

    # 생성 후 사용
    from cosmos_objects import Customer, Order
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

import httpx

from cosmos_sdk._internal.types import LinkType, ObjectType
from cosmos_sdk.codegen.generator import CodeGenerator
from cosmos_sdk.dataset.auth import AuthManager
from cosmos_sdk.dataset.connection import get_connection_from_env, parse_connection_string


# ──────────────────────────────────────────────
# API fetcher
# ──────────────────────────────────────────────

async def _fetch_schema(
    base_url: str,
    email: str,
    password: str,
    graph_key: str,
    timeout: float = 30.0,
) -> tuple[list[ObjectType], list[LinkType]]:
    """API Gateway에서 Object/Link 타입 정의 fetch."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        auth = AuthManager(base_url, email, password)
        token = await auth.get_token(client)
        headers = {"Authorization": f"Bearer {token}"}

        # Object types
        obj_resp = await client.get(
            f"{base_url}/api/v1/objects/object-types",
            headers=headers,
            params={"tenantId": graph_key},
        )
        obj_resp.raise_for_status()
        obj_data: list[dict[str, Any]] = obj_resp.json().get("data") or []

        # Link types
        lnk_resp = await client.get(
            f"{base_url}/api/v1/objects/link-types",
            headers=headers,
            params={"tenantId": graph_key},
        )
        lnk_resp.raise_for_status()
        lnk_data: list[dict[str, Any]] = lnk_resp.json().get("data") or []

    object_types = [ObjectType.model_validate(t) for t in obj_data]
    link_types = [LinkType.model_validate(t) for t in lnk_data]
    return object_types, link_types


# ──────────────────────────────────────────────
# generate 커맨드
# ──────────────────────────────────────────────

async def _cmd_generate(args: argparse.Namespace) -> int:
    # 인증 정보 resolve
    base_url: str
    email: str
    password: str

    if args.connection:
        conn = parse_connection_string(args.connection)
        base_url, email, password = conn.base_url, conn.email, conn.password
    elif args.base_url and args.email and args.password:
        base_url, email, password = args.base_url, args.email, args.password
    else:
        env_conn = get_connection_from_env()
        if env_conn:
            base_url, email, password = env_conn.base_url, env_conn.email, env_conn.password
        else:
            print(
                "Error: --connection 또는 (--base-url + --email + --password)가 필요합니다.\n"
                "환경변수 COSMOS_CONNECTION_STRING도 사용할 수 있습니다.",
                file=sys.stderr,
            )
            return 1

    graph_key: str = args.graph
    output_dir = Path(args.output)

    print(f"[cosmos-codegen] {base_url} / graph={graph_key}")
    print(f"[cosmos-codegen] Object/Link 타입 fetch 중...")

    try:
        object_types, link_types = await _fetch_schema(base_url, email, password, graph_key)
    except httpx.HTTPStatusError as e:
        print(f"Error: API 요청 실패 ({e.response.status_code}): {e.response.text}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if not object_types:
        print(f"[cosmos-codegen] graph '{graph_key}'에 Object 타입이 없습니다.")
        return 0

    print(f"[cosmos-codegen] Object {len(object_types)}개, Link {len(link_types)}개 발견")
    print(f"[cosmos-codegen] 파일 생성 중 → {output_dir}/")

    generator = CodeGenerator(output_dir)
    result = generator._generate(object_types, link_types)

    # 결과 출력
    all_files = result["objects"] + result["links"]
    unique_files = sorted(set(all_files))
    for path in unique_files:
        print(f"  생성: {path}")

    print(f"\n[cosmos-codegen] 완료: {len(unique_files)}개 파일")
    print(f"\n사용 방법:")
    print(f"  import sys; sys.path.insert(0, '{output_dir.parent}')")

    # 생성된 Object 클래스 이름 나열
    class_names = [_to_pascal(t.name) for t in object_types]
    print(f"  from {output_dir.name} import {', '.join(class_names[:3])}" + (
        f", ..." if len(class_names) > 3 else ""
    ))
    return 0


def _to_pascal(name: str) -> str:
    import re
    return "".join(w.capitalize() for w in re.split(r"[_\s]+", name) if w)


# ──────────────────────────────────────────────
# list 커맨드
# ──────────────────────────────────────────────

async def _cmd_list(args: argparse.Namespace) -> int:
    """Object/Link 타입 목록만 출력 (파일 생성 없음)."""
    if args.connection:
        conn = parse_connection_string(args.connection)
        base_url, email, password = conn.base_url, conn.email, conn.password
    elif args.base_url and args.email and args.password:
        base_url, email, password = args.base_url, args.email, args.password
    else:
        env_conn = get_connection_from_env()
        if env_conn:
            base_url, email, password = env_conn.base_url, env_conn.email, env_conn.password
        else:
            print("Error: 인증 정보가 없습니다.", file=sys.stderr)
            return 1

    try:
        object_types, link_types = await _fetch_schema(base_url, email, password, args.graph)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    print(f"\nGraph: {args.graph}")
    print(f"\nObjects ({len(object_types)}):")
    for t in object_types:
        props = len(t.properties) if t.properties else 0
        print(f"  {t.name:30s}  ({props} properties)")

    print(f"\nLinks ({len(link_types)}):")
    for t in link_types:
        print(f"  {t.name:30s}  {t.source_type} → {t.target_type}  [{t.cardinality}]")

    return 0


# ──────────────────────────────────────────────
# 공통 인증 인자 추가 헬퍼
# ──────────────────────────────────────────────

def _add_auth_args(parser: argparse.ArgumentParser) -> None:
    grp = parser.add_argument_group("인증")
    grp.add_argument(
        "--connection", "-c",
        metavar="STRING",
        help='Connection string. 예: "cosmos://admin%%40cosmos.local:pass%%40@localhost:3001"',
    )
    grp.add_argument("--base-url", metavar="URL", help="API 서버 URL. 예: http://localhost:3001")
    grp.add_argument("--email", metavar="EMAIL")
    grp.add_argument("--password", metavar="PASS")
    grp.add_argument(
        "--graph", "-g",
        required=True,
        metavar="GRAPH_KEY",
        help="Graph key. 예: graph_my_org",
    )


# ──────────────────────────────────────────────
# main
# ──────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="cosmos-codegen",
        description="Cosmos Object/Link 타입에서 Python 클래스 생성",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # generate
    gen_parser = subparsers.add_parser("generate", help="Python 클래스 파일 생성")
    _add_auth_args(gen_parser)
    gen_parser.add_argument(
        "--output", "-o",
        required=True,
        metavar="DIR",
        help="출력 디렉토리. 예: ./cosmos_objects",
    )

    # list
    list_parser = subparsers.add_parser("list", help="Object/Link 타입 목록 출력")
    _add_auth_args(list_parser)

    args = parser.parse_args()

    if args.command == "generate":
        sys.exit(asyncio.run(_cmd_generate(args)))
    elif args.command == "list":
        sys.exit(asyncio.run(_cmd_list(args)))


if __name__ == "__main__":
    main()
