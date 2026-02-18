"""Deprecated: Use cosmos_sdk.action instead."""
import warnings
warnings.warn(
    "cosmos_sdk.object_action is deprecated, use cosmos_sdk.action instead",
    DeprecationWarning,
    stacklevel=2,
)
from cosmos_sdk.action import *
