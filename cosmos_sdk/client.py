"""
Cosmos SDK Client.

Main entry point for interacting with the Cosmos Object Graph.
"""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Any, TypeVar

from cosmos_sdk._internal.api import ObjectDBClient
from cosmos_sdk._internal.types import (
    ClearOverrideResult,
    CreateObjectResult,
    OverrideChange,
    OverrideResult,
)
from cosmos_sdk.base import BaseObject, ObjectSet

if TYPE_CHECKING:
    pass

T = TypeVar("T", bound=BaseObject)

# Singleton instance
_client_instance: "CosmosClient | None" = None


class ObjectsAccessor:
    """
    Accessor for Object types with lazy loading.

    Provides attribute-style access to Object types:
        client.objects.Customer
        client.objects.Order

    Objects are loaded on-demand from the SDK path when first accessed.
    """

    def __init__(self, client: CosmosClient, object_registry: dict[str, type[BaseObject]]):
        self._client = client
        self._registry = object_registry

    _last_load_debug: list[str] = []  # Class-level debug storage

    def _try_load_from_sdk(self, name: str) -> type[BaseObject] | None:
        """Try to load an Object class from the graph SDK directory."""
        import importlib

        # Reset debug info
        self._last_load_debug = []
        debug = self._last_load_debug

        # client._tenant_id 우선, fallback으로 환경변수
        graph_key = getattr(self._client, "_tenant_id", None) or os.environ.get("COSMOS_GRAPH_KEY", "")
        debug.append(f"load_attempt_for='{name}'")

        if not graph_key:
            debug.append("GRAPH_KEY_NOT_SET")
            return None

        sdk_base_path = os.environ.get("COSMOS_SDK_PATH", "/shared/python-sdk")
        safe_graph_key = graph_key.replace("/", "_").replace("\\", "_").replace("..", "_")
        sdk_path = os.path.join(sdk_base_path, safe_graph_key)

        if not os.path.exists(sdk_path):
            debug.append(f"SDK_PATH_NOT_EXISTS={sdk_path}")
            return None

        # Add SDK base path to sys.path if needed
        if sdk_base_path not in sys.path:
            sys.path.insert(0, sdk_base_path)
            debug.append("ADDED_TO_SYS_PATH")

        # Try to import the class from the graph module
        try:
            debug.append(f"importing_module='{safe_graph_key}'")
            graph_module = importlib.import_module(safe_graph_key)
            debug.append(f"module_imported={graph_module}")

            exported = getattr(graph_module, "__all__", [])
            debug.append(f"module_all={exported}")

            # Check if the class is exported
            if name in exported:
                obj_class = getattr(graph_module, name, None)
                debug.append(f"found_class={obj_class}")
                if obj_class and isinstance(obj_class, type) and issubclass(obj_class, BaseObject):
                    debug.append("SUCCESS_EXACT_MATCH")
                    return obj_class
                else:
                    debug.append(f"NOT_BASEOBJECT: is_type={isinstance(obj_class, type)}")

            # Try case-insensitive match
            for exported_name in exported:
                if exported_name.lower() == name.lower():
                    obj_class = getattr(graph_module, exported_name, None)
                    debug.append(f"found_case_insensitive={exported_name}->{obj_class}")
                    if obj_class and isinstance(obj_class, type) and issubclass(obj_class, BaseObject):
                        debug.append("SUCCESS_CASE_INSENSITIVE")
                        return obj_class

            debug.append(f"NOT_FOUND_IN_EXPORTS")

        except ImportError as e:
            debug.append(f"IMPORT_ERROR={e}")
        except Exception as e:
            debug.append(f"UNEXPECTED_ERROR={type(e).__name__}:{e}")

        return None

    def __getattr__(self, name: str) -> ObjectSet:
        """Get ObjectSet for the named type (lazy loading)."""
        import logging
        logger = logging.getLogger("cosmos_sdk.client")

        if name.startswith("_"):
            raise AttributeError(name)

        logger.debug(f"[ObjectsAccessor] __getattr__ called for '{name}'")
        logger.debug(f"[ObjectsAccessor] Current registry keys: {list(self._registry.keys())}")

        # Check registry first
        if name in self._registry:
            object_type = self._registry[name]
            return ObjectSet(
                self._client,
                object_type,
                getattr(object_type, '__object_type__', '') or name.lower(),
            )

        # Try to load from SDK (lazy loading)
        object_type = self._try_load_from_sdk(name)
        if object_type:
            # Cache in registry for future access
            self._registry[name] = object_type
            return ObjectSet(
                self._client,
                object_type,
                getattr(object_type, '__object_type__', '') or name.lower(),
            )

        # Try case-insensitive match in registry
        for key in self._registry:
            if key.lower() == name.lower():
                object_type = self._registry[key]
                return ObjectSet(
                    self._client,
                    object_type,
                    getattr(object_type, '__object_type__', '') or key.lower(),
                )

        # Collect debug info for error message
        graph_key = os.environ.get("COSMOS_GRAPH_KEY", "")
        sdk_base_path = os.environ.get("COSMOS_SDK_PATH", "/shared/python-sdk")
        safe_graph_key = graph_key.replace("/", "_").replace("\\", "_").replace("..", "_") if graph_key else ""
        sdk_path = os.path.join(sdk_base_path, safe_graph_key) if safe_graph_key else ""

        debug_info = []
        debug_info.append(f"COSMOS_GRAPH_KEY='{graph_key}'")
        debug_info.append(f"COSMOS_SDK_PATH='{sdk_base_path}'")
        debug_info.append(f"sdk_path='{sdk_path}'")
        debug_info.append(f"sdk_path_exists={os.path.exists(sdk_path) if sdk_path else False}")

        if sdk_path and os.path.exists(sdk_path):
            try:
                debug_info.append(f"sdk_contents={os.listdir(sdk_path)}")
            except Exception as e:
                debug_info.append(f"sdk_contents_error={e}")

        if sdk_base_path and os.path.exists(sdk_base_path):
            try:
                debug_info.append(f"sdk_base_contents={os.listdir(sdk_base_path)}")
            except Exception:
                pass

        debug_info.append(f"sys.path[0:5]={sys.path[0:5]}")
        debug_info.append(f"registry_keys={list(self._registry.keys())}")
        debug_info.append(f"load_debug=[{', '.join(self._last_load_debug)}]")

        raise AttributeError(
            f"Unknown Object type: {name}. "
            f"Debug: {'; '.join(debug_info)}"
        )

    def __dir__(self) -> list[str]:
        """List available Object types."""
        return list(self._registry.keys())

    def register(self, object_type: type[BaseObject]) -> None:
        """Register an Object type."""
        name = object_type.__name__
        self._registry[name] = object_type

    def list_types(self) -> list[str]:
        """List all registered Object type names."""
        return list(self._registry.keys())


class ObjectDBAccessor:
    """
    Accessor for ObjectDB Override API.

    Provides methods for object state transitions used by Object Actions:
        - override: Apply state changes to objects
        - create_object: Create new objects
        - delete_object: Delete an object
        - clear_override: Revert to Fact values

    Example:
        result = await cosmos.objectdb.override(
            object_type="Order",
            object_ids=["order_123"],
            changes=[
                OverrideChange(property="status", op="TRANSITION", value="APPROVED")
            ],
            action_id="job_xxx"
        )
    """

    def __init__(self, api_client: ObjectDBClient):
        self._api_client = api_client

    async def override(
        self,
        object_type: str,
        object_ids: list[str],
        changes: list[OverrideChange],
        action_id: str | None = None,
        tenant_id: str | None = None,
    ) -> OverrideResult:
        return await self._api_client.override(
            object_type=object_type,
            object_ids=object_ids,
            changes=changes,
            action_id=action_id,
            tenant_id=tenant_id,
        )

    async def create_object(
        self,
        object_type: str,
        object_id: str,
        properties: dict,
        tenant_id: str | None = None,
    ) -> CreateObjectResult:
        return await self._api_client.create_object(
            object_type=object_type,
            object_id=object_id,
            properties=properties,
            tenant_id=tenant_id,
        )

    async def clear_override(
        self,
        object_type: str,
        object_ids: list[str],
        properties: list[str],
        tenant_id: str | None = None,
    ) -> ClearOverrideResult:
        return await self._api_client.clear_override(
            object_type=object_type,
            object_ids=object_ids,
            properties=properties,
            tenant_id=tenant_id,
        )

    async def delete_object(
        self,
        object_type: str,
        object_id: str,
        tenant_id: str | None = None,
    ) -> None:
        await self._api_client.delete_object(
            object_type=object_type,
            object_id=object_id,
            tenant_id=tenant_id,
        )


class _AuthManagedObjectDBClient(ObjectDBClient):
    """
    ObjectDBClient 확장 — AuthManager로 토큰을 자동 갱신.

    connection_string 모드에서 API Gateway를 통해 ObjectDB에 접근할 때 사용.
    """

    def __init__(self, base_url: str, auth_manager: Any, timeout: float):
        super().__init__(base_url=base_url, token=None, timeout=timeout)
        self._auth_manager = auth_manager

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        # jwt_token이 명시적으로 주어지지 않은 경우 AuthManager에서 토큰 획득
        if kwargs.get("jwt_token") is None:
            client = await self._get_client()
            self.token = await self._auth_manager.get_token(client)
        return await super()._request(method, path, **kwargs)


class CosmosClient:
    """
    Cosmos SDK 통합 클라이언트.

    Object 쿼리(client.objects)와 Dataset CRUD(client.datasets)를 단일 인터페이스로 제공.

    인증 방식:
        # 외부 사용 — connection string (자동 로그인/갱신)
        client = CosmosClient("cosmos://admin%40cosmos.local:admin123%40@localhost:3001")

        # 외부 사용 — 명시적 파라미터
        client = CosmosClient(
            base_url="http://localhost:3001",
            email="admin@cosmos.local",
            password="admin123@",
        )

        # 내부 실행환경 — 기존 token 방식 (하위 호환)
        client = CosmosClient(token="eyJ...")

    사용 예시:
        # Object 쿼리 (codegen 클래스 또는 lazy 로딩)
        customers = await client.objects.Customer.where(
            Customer.status == "active"
        ).list()

        # Dataset CRUD (connection string 모드에서만 사용 가능)
        df = await client.datasets.get_dataframe("my_dataset")
        await client.datasets.overwrite_table("my_dataset", transformed_df)
    """

    _initialized: bool = False

    def __new__(
        cls,
        connection_string: str | None = None,
        *,
        graph: str | None = None,
        token: str | None = None,
        base_url: str | None = None,
        email: str | None = None,
        password: str | None = None,
        timeout: float = 30.0,
        batch_size: int = 5000,
    ) -> "CosmosClient":
        """Return singleton instance (create if needed)."""
        global _client_instance
        if _client_instance is None:
            _client_instance = super().__new__(cls)
            _client_instance._initialized = False
        return _client_instance

    def __init__(
        self,
        connection_string: str | None = None,
        *,
        graph: str | None = None,
        token: str | None = None,
        base_url: str | None = None,
        email: str | None = None,
        password: str | None = None,
        timeout: float = 30.0,
        batch_size: int = 5000,
    ):
        """
        Args:
            connection_string: "cosmos://email:password@host:port" 형식.
                환경변수 COSMOS_CONNECTION_STRING도 지원.
            graph: Graph key. Object 쿼리 범위를 특정 그래프로 한정.
                생략 시 COSMOS_GRAPH_KEY 환경변수 사용.
            token: JWT 토큰 직접 주입 (내부 실행환경용, 하위 호환).
            base_url: 명시적 API URL.
            email: 이메일 (base_url, password와 함께 사용).
            password: 패스워드.
            timeout: HTTP 타임아웃 (초).
            batch_size: Dataset 배치 크기 (기본 5,000행).
        """
        if getattr(self, "_initialized", False):
            return

        self._timeout = timeout

        # ── 외부 모드: connection string / email+password ──────────────
        if connection_string or email or os.environ.get("COSMOS_CONNECTION_STRING"):
            from cosmos_sdk.dataset.auth import AuthManager
            from cosmos_sdk.dataset.api import DatasetAPIClient
            from cosmos_sdk.dataset.client import DatasetClient
            from cosmos_sdk.dataset.connection import (
                get_connection_from_env,
                parse_connection_string,
            )

            if connection_string:
                conn = parse_connection_string(connection_string)
            elif base_url and email and password:
                from cosmos_sdk.dataset.connection import CosmosConnection
                conn = CosmosConnection(email=email, password=password, base_url=base_url)
            else:
                conn = get_connection_from_env()
                if conn is None:
                    raise ValueError(
                        "connection_string, (base_url + email + password), 또는 "
                        "COSMOS_CONNECTION_STRING 환경변수가 필요합니다."
                    )

            self._tenant_id = None
            self._auth_manager = AuthManager(conn.base_url, conn.email, conn.password)
            # ObjectDB는 API Gateway의 /objects/* 프록시를 통해 접근
            self._api_client = _AuthManagedObjectDBClient(
                conn.base_url, self._auth_manager, timeout
            )
            # Dataset accessor
            _ds_api = DatasetAPIClient(self._auth_manager, timeout=timeout)
            self.datasets: DatasetClient | None = DatasetClient._from_components(
                self._auth_manager, _ds_api, batch_size, graph_key=self._tenant_id
            )

        # ── 내부 모드: token 직접 주입 (하위 호환) ────────────────────
        else:
            self._tenant_id = None
            self._auth_manager = None
            _token = token or os.environ.get("COSMOS_AUTH_TOKEN") or os.environ.get("AUTH_TOKEN")
            _objectdb_url = base_url or os.environ.get("COSMOS_API_URL", "http://localhost:8080")
            self._api_client = ObjectDBClient(
                base_url=_objectdb_url,
                token=_token,
                timeout=timeout,
            )
            self.datasets = None  # token 모드에서는 built-in function 사용

        # Object type registry (공통)
        self._object_registry: dict[str, type[BaseObject]] = {}
        self._load_default_objects()
        self.objects = ObjectsAccessor(self, self._object_registry)
        self.objectdb = ObjectDBAccessor(self._api_client)

        self._initialized = True

    def _load_default_objects(self) -> None:
        """Load default Object types from cosmos_sdk.objects."""
        try:
            import importlib
            from cosmos_sdk import objects

            importlib.reload(objects)

            for name in getattr(objects, "__all__", []):
                obj_class = getattr(objects, name, None)
                if obj_class and isinstance(obj_class, type) and issubclass(obj_class, BaseObject):
                    self._object_registry[name] = obj_class
        except ImportError:
            pass

    def register_objects(self, *object_types: type[BaseObject]) -> None:
        """Register additional Object types."""
        for obj_type in object_types:
            self._object_registry[obj_type.__name__] = obj_type

    async def health(self) -> dict[str, str]:
        """Check service health."""
        return await self._api_client.health()

    async def close(self) -> None:
        """Close the client and release resources."""
        await self._api_client.close()
        if self.datasets is not None:
            await self.datasets.close()

    async def __aenter__(self) -> CosmosClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()


def create_client(
    connection_string: str | None = None,
    *,
    graph: str | None = None,
    token: str | None = None,
    base_url: str | None = None,
    email: str | None = None,
    password: str | None = None,
) -> CosmosClient:
    """
    Create a new Cosmos client.

    Args:
        connection_string: "cosmos://email:password@host:port"
        graph: Graph key (Object 쿼리 범위 한정)
        token: JWT 토큰 (내부 실행환경용)
        base_url: API URL
        email: 이메일
        password: 패스워드
    """
    return CosmosClient(
        connection_string,
        graph=graph,
        token=token,
        base_url=base_url,
        email=email,
        password=password,
    )
