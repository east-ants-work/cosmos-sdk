"""
cosmos_sdk.spark — Read-only PySpark SDK for Cosmos Object Graph.

Provides SparkClient and SparkObjectSet for accessing Object data
as PySpark DataFrames in Spark execution environments (e.g., Dataproc).

Usage:
    from cosmos_sdk.spark import SparkClient

    client = SparkClient(spark=spark, backend="iceberg")
    df = client.objects.Customer.to_dataframe()
"""

from cosmos_sdk.spark.client import SparkClient
from cosmos_sdk.spark.object_set import SparkObjectSet

__all__ = [
    "SparkClient",
    "SparkObjectSet",
]
