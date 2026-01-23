"""
Generated Object classes for Cosmos SDK.

This module dynamically loads Object types from the graph-specific SDK directory.
Objects are generated per Graph and stored in /shared/python-sdk/{graph_key}/.

Usage:
    from cosmos_sdk.objects import Factories, Orders
    # or
    from cosmos_sdk.objects import __all__
    print(__all__)  # ["Factories", "Orders", ...]
"""

import os
import sys
import importlib
from typing import List

__all__: List[str] = []

def _load_graph_objects():
    """
    Dynamically load Object classes from the graph-specific SDK directory.

    The SDK path and graph key are determined from environment variables:
    - COSMOS_SDK_PATH: Base path for SDK files (default: /shared/python-sdk)
    - COSMOS_GRAPH_KEY: Current graph key (set by code transformer)
    """
    global __all__

    sdk_base_path = os.environ.get("COSMOS_SDK_PATH", "/shared/python-sdk")
    graph_key = os.environ.get("COSMOS_GRAPH_KEY", "")

    if not graph_key:
        # No graph key set, objects not available
        return

    # Sanitize graph key for path
    safe_graph_key = graph_key.replace("/", "_").replace("\\", "_").replace("..", "_")
    sdk_path = os.path.join(sdk_base_path, safe_graph_key)

    if not os.path.exists(sdk_path):
        # SDK not generated yet for this graph
        return

    # Add SDK path to sys.path if not already there
    if sdk_base_path not in sys.path:
        sys.path.insert(0, sdk_base_path)

    try:
        # Import the graph-specific SDK module
        graph_module = importlib.import_module(safe_graph_key)

        # Get all exported classes
        exported = getattr(graph_module, "__all__", [])

        # Import each class into this module's namespace
        for name in exported:
            obj_class = getattr(graph_module, name, None)
            if obj_class is not None:
                globals()[name] = obj_class
                __all__.append(name)

    except ImportError:
        # Graph SDK not available
        pass

# Load objects on module import
_load_graph_objects()
