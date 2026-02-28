"""
cosmos_sdk.dataset — Internal Dataset 외부 CRUD SDK.

사용 예시:
    from cosmos_sdk.dataset import DatasetClient

    client = DatasetClient("cosmos://admin%40cosmos.local:admin123%40@localhost:3001")
    df = client.get_dataframe_sync("my_dataset")
    client.overwrite_table_sync("my_dataset", transformed_df)
    client.close_sync()
"""

from cosmos_sdk.dataset.client import DatasetClient
from cosmos_sdk.dataset.connection import CosmosConnection, parse_connection_string
from cosmos_sdk.dataset.errors import AuthError, BatchError, DatasetError, NotFoundError
from cosmos_sdk.dataset.errors import PermissionError as DatasetPermissionError
from cosmos_sdk.dataset.types import FilterSpec, RowsResult

__all__ = [
    "DatasetClient",
    "CosmosConnection",
    "parse_connection_string",
    # Errors
    "DatasetError",
    "AuthError",
    "DatasetPermissionError",
    "NotFoundError",
    "BatchError",
    # Types
    "FilterSpec",
    "RowsResult",
]
