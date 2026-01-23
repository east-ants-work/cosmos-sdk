"""
Main code generator for Cosmos SDK.

Orchestrates generation of Python Object classes from ObjectDB schema.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from cosmos_sdk._internal.api import ObjectDBClient
from cosmos_sdk._internal.types import LinkType, ObjectType
from cosmos_sdk.codegen.link_generator import LinkTypeGenerator
from cosmos_sdk.codegen.object_generator import ObjectTypeGenerator


class CodeGenerator:
    """
    Main code generator for Cosmos SDK.

    Generates Python Object classes from ObjectDB schema definitions.
    Can fetch definitions from API or load from JSON files.

    Usage:
        # From API
        generator = CodeGenerator(output_dir="cosmos_sdk/objects")
        await generator.generate_from_api(api_client)

        # From JSON files
        generator = CodeGenerator(output_dir="cosmos_sdk/objects")
        generator.generate_from_json(
            object_types_file="object_types.json",
            link_types_file="link_types.json",
        )
    """

    def __init__(self, output_dir: str | Path):
        """
        Initialize generator.

        Args:
            output_dir: Directory to write generated files
        """
        self.output_dir = Path(output_dir)
        self.object_generator = ObjectTypeGenerator(self.output_dir)
        self.link_generator = LinkTypeGenerator(self.output_dir)

    async def generate_from_api(
        self,
        api_client: ObjectDBClient,
        tenant_id: str | None = None,
    ) -> dict[str, list[Path]]:
        """
        Generate all Object classes from API.

        Args:
            api_client: ObjectDB API client
            tenant_id: Optional tenant ID filter

        Returns:
            Dictionary with 'objects' and 'links' keys containing generated file paths
        """
        # Fetch object types
        object_types_response = await api_client.list_object_types_internal(tenant_id)
        object_types = object_types_response.object_types

        # Fetch link types
        link_types_response = await api_client.list_link_types(tenant_id)
        link_types = link_types_response.link_types

        return self._generate(object_types, link_types)

    def generate_from_json(
        self,
        object_types_file: str | Path | None = None,
        link_types_file: str | Path | None = None,
        object_types_data: list[dict[str, Any]] | None = None,
        link_types_data: list[dict[str, Any]] | None = None,
    ) -> dict[str, list[Path]]:
        """
        Generate Object classes from JSON data.

        Args:
            object_types_file: Path to JSON file with ObjectType definitions
            link_types_file: Path to JSON file with LinkType definitions
            object_types_data: ObjectType definitions as list of dicts
            link_types_data: LinkType definitions as list of dicts

        Returns:
            Dictionary with 'objects' and 'links' keys containing generated file paths
        """
        # Load object types
        object_types: list[ObjectType] = []
        if object_types_file:
            with open(object_types_file, encoding="utf-8") as f:
                data = json.load(f)
                for item in data:
                    object_types.append(ObjectType.model_validate(item))
        elif object_types_data:
            for item in object_types_data:
                object_types.append(ObjectType.model_validate(item))

        # Load link types
        link_types: list[LinkType] = []
        if link_types_file:
            with open(link_types_file, encoding="utf-8") as f:
                data = json.load(f)
                for item in data:
                    link_types.append(LinkType.model_validate(item))
        elif link_types_data:
            for item in link_types_data:
                link_types.append(LinkType.model_validate(item))

        return self._generate(object_types, link_types)

    def _generate(
        self,
        object_types: list[ObjectType],
        link_types: list[LinkType],
    ) -> dict[str, list[Path]]:
        """
        Generate all files.

        Args:
            object_types: List of ObjectType definitions
            link_types: List of LinkType definitions

        Returns:
            Dictionary with generated file paths
        """
        result: dict[str, list[Path]] = {
            "objects": [],
            "links": [],
        }

        # Generate object classes first
        for obj_type in object_types:
            path = self.object_generator.generate(obj_type)
            result["objects"].append(path)

        # Generate __init__.py
        if object_types:
            init_path = self.object_generator.generate_init_file(object_types)
            result["objects"].append(init_path)

        # Add links to object classes
        for link_type in link_types:
            source_path, target_path = self.link_generator.add_link(link_type)
            if source_path.exists():
                result["links"].append(source_path)
            if target_path.exists():
                result["links"].append(target_path)

        return result

    def generate_single_object(self, object_type: ObjectType | dict[str, Any]) -> Path:
        """
        Generate a single Object class.

        Args:
            object_type: ObjectType definition or dict

        Returns:
            Path to generated file
        """
        if isinstance(object_type, dict):
            object_type = ObjectType.model_validate(object_type)
        return self.object_generator.generate(object_type)

    def add_single_link(self, link_type: LinkType | dict[str, Any]) -> tuple[Path, Path]:
        """
        Add a single Link to Object classes.

        Args:
            link_type: LinkType definition or dict

        Returns:
            Tuple of (source_file, target_file) paths
        """
        if isinstance(link_type, dict):
            link_type = LinkType.model_validate(link_type)
        return self.link_generator.add_link(link_type)
