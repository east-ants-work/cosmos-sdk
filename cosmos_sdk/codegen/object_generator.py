"""
ObjectType to Python class generator.

Generates Python class files from ObjectType definitions.
"""

from __future__ import annotations

import keyword
import re
from pathlib import Path
from typing import Any

from cosmos_sdk._internal.types import ObjectType, PropertyDefinition, PropertyType


# Map ObjectDB property types to Python type hints
PROPERTY_TYPE_MAP = {
    PropertyType.STRING: "str",
    PropertyType.INT: "int",
    PropertyType.FLOAT: "float",
    PropertyType.BOOL: "bool",
    PropertyType.DATETIME: "datetime",
    PropertyType.JSON: "dict[str, Any]",
    PropertyType.ARRAY: "list[Any]",
}


def to_snake_case(name: str) -> str:
    """Convert CamelCase to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def to_pascal_case(name: str) -> str:
    """Convert snake_case to PascalCase."""
    return "".join(word.capitalize() for word in name.split("_"))


def safe_identifier(name: str) -> str:
    """Make a name safe for use as Python identifier."""
    # Replace invalid characters
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    # Ensure doesn't start with number
    if safe and safe[0].isdigit():
        safe = "_" + safe
    # Handle Python keywords
    if keyword.iskeyword(safe):
        safe = safe + "_"
    return safe


class ObjectTypeGenerator:
    """
    Generates Python class files from ObjectType definitions.

    Usage:
        generator = ObjectTypeGenerator(output_dir="cosmos_sdk/objects")
        generator.generate(object_type)
    """

    def __init__(self, output_dir: str | Path):
        """
        Initialize generator.

        Args:
            output_dir: Directory to write generated files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, object_type: ObjectType) -> Path:
        """
        Generate Python class file for an ObjectType.

        Args:
            object_type: ObjectType definition

        Returns:
            Path to generated file
        """
        class_name = to_pascal_case(object_type.name)
        file_name = to_snake_case(object_type.name) + ".py"
        file_path = self.output_dir / file_name

        code = self._generate_code(object_type, class_name)
        file_path.write_text(code, encoding="utf-8")

        return file_path

    def _generate_code(self, object_type: ObjectType, class_name: str) -> str:
        """Generate the Python code for an ObjectType."""
        lines: list[str] = []

        # Module docstring
        lines.append('"""')
        lines.append(f"{class_name}: {object_type.description or object_type.name}")
        lines.append("")
        if object_type.description:
            lines.append(f"Description: {object_type.description}")
        lines.append('"""')
        lines.append("")

        # Imports
        lines.append("from __future__ import annotations")
        lines.append("")
        lines.append("from typing import TYPE_CHECKING, Any")
        lines.append("")
        lines.append("from cosmos_sdk.base import BaseObject, Link, Property")
        lines.append("")

        # TYPE_CHECKING imports for Links (will be populated by LinkTypeGenerator)
        lines.append("if TYPE_CHECKING:")
        lines.append("    pass  # Link target imports will be added here")
        lines.append("")
        lines.append("")

        # Class definition
        lines.append(f"class {class_name}(BaseObject):")

        # Class docstring
        lines.append('    """')
        lines.append(f"    {class_name} Object.")
        lines.append("")
        if object_type.description:
            lines.append(f"    {object_type.description}")
            lines.append("")

        # Document properties
        lines.append("    Properties:")
        for prop in object_type.properties:
            prop_name = safe_identifier(prop.key or prop.name)
            prop_desc = prop.description or prop.name
            lines.append(f"        {prop_name}: {prop_desc}")
        lines.append('    """')
        lines.append("")

        # Class attributes
        lines.append(f'    __object_type_key__ = "{object_type.type_key}"')

        # Find primary key
        primary_key = object_type.primary_key or "id"
        lines.append(f'    __primary_key__ = "{primary_key}"')
        lines.append("")

        # Properties
        lines.append("    # Properties")
        for prop in object_type.properties:
            prop_line = self._generate_property(prop, primary_key)
            lines.append(f"    {prop_line}")
        lines.append("")

        # Links placeholder
        lines.append("    # Links (populated by LinkTypeGenerator)")
        lines.append("")

        return "\n".join(lines)

    def _generate_property(
        self,
        prop: PropertyDefinition,
        primary_key: str,
    ) -> str:
        """Generate a Property definition line."""
        prop_name = safe_identifier(prop.key or prop.name)
        is_pk = prop_name == primary_key or (prop.key and prop.key == primary_key)

        args: list[str] = []
        args.append(f'type="{prop.type.value}"')

        if is_pk:
            args.append("primary_key=True")
        if prop.required:
            args.append("required=True")
        if prop.indexed:
            args.append("indexed=True")
        if prop.description:
            # Escape quotes in description
            desc = prop.description.replace('"', '\\"')
            args.append(f'description="{desc}"')

        return f'{prop_name} = Property({", ".join(args)})'

    def generate_from_dict(self, data: dict[str, Any]) -> Path:
        """
        Generate from a dictionary (e.g., from JSON).

        Args:
            data: ObjectType as dictionary

        Returns:
            Path to generated file
        """
        object_type = ObjectType.model_validate(data)
        return self.generate(object_type)

    def generate_init_file(self, object_types: list[ObjectType]) -> Path:
        """
        Generate or update objects/__init__.py.

        Args:
            object_types: List of ObjectTypes to export

        Returns:
            Path to __init__.py
        """
        init_path = self.output_dir / "__init__.py"

        lines: list[str] = []
        lines.append('"""')
        lines.append("Generated Object classes for Cosmos SDK.")
        lines.append("")
        lines.append("This module exports all Object types available in the current Graph.")
        lines.append('"""')
        lines.append("")

        # Imports
        class_names: list[str] = []
        for obj_type in object_types:
            class_name = to_pascal_case(obj_type.name)
            module_name = to_snake_case(obj_type.name)
            lines.append(f"from cosmos_sdk.objects.{module_name} import {class_name}")
            class_names.append(class_name)

        lines.append("")

        # __all__
        all_items = ", ".join(f'"{name}"' for name in class_names)
        lines.append(f"__all__ = [{all_items}]")
        lines.append("")

        init_path.write_text("\n".join(lines), encoding="utf-8")
        return init_path
