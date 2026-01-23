"""
LinkType to Python Link member generator.

Updates Object class files to include Link definitions.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from cosmos_sdk._internal.types import Cardinality, LinkType
from cosmos_sdk.codegen.object_generator import to_pascal_case, to_snake_case, safe_identifier


class LinkTypeGenerator:
    """
    Generates Link members from LinkType definitions.

    Updates existing Object class files to include Link definitions.

    Usage:
        generator = LinkTypeGenerator(objects_dir="cosmos_sdk/objects")
        generator.add_link(link_type)
    """

    def __init__(self, objects_dir: str | Path):
        """
        Initialize generator.

        Args:
            objects_dir: Directory containing Object class files
        """
        self.objects_dir = Path(objects_dir)

    def add_link(self, link_type: LinkType) -> tuple[Path, Path]:
        """
        Add Link to both source and target Object classes.

        Args:
            link_type: LinkType definition

        Returns:
            Tuple of (source_file, target_file) paths that were modified
        """
        source_class = to_pascal_case(link_type.source_type)
        target_class = to_pascal_case(link_type.target_type)
        source_file = self.objects_dir / f"{to_snake_case(link_type.source_type)}.py"
        target_file = self.objects_dir / f"{to_snake_case(link_type.target_type)}.py"

        # Determine link names
        forward_link_name = safe_identifier(link_type.name)
        reverse_link_name = self._get_reverse_link_name(link_type, source_class)

        # Determine cardinality
        is_many = link_type.cardinality == Cardinality.ONE_TO_MANY

        # Update source class (forward link)
        if source_file.exists():
            self._add_link_to_file(
                source_file,
                link_name=forward_link_name,
                target_type=target_class,
                many=is_many,
                reverse=reverse_link_name,
                target_module=to_snake_case(link_type.target_type),
            )

        # Update target class (reverse link)
        if target_file.exists():
            self._add_link_to_file(
                target_file,
                link_name=reverse_link_name,
                target_type=source_class,
                many=True,  # Reverse is always many
                reverse=forward_link_name,
                target_module=to_snake_case(link_type.source_type),
            )

        return source_file, target_file

    def _get_reverse_link_name(self, link_type: LinkType, source_class: str) -> str:
        """Generate a reasonable reverse link name."""
        # Use source class name in plural/lowercase as reverse link name
        name = to_snake_case(source_class)
        # Simple pluralization
        if not name.endswith("s"):
            name += "s"
        return name

    def _add_link_to_file(
        self,
        file_path: Path,
        link_name: str,
        target_type: str,
        many: bool,
        reverse: str,
        target_module: str,
    ) -> None:
        """Add a Link definition to an Object class file."""
        content = file_path.read_text(encoding="utf-8")

        # Check if link already exists
        if f"{link_name} = Link(" in content or f"{link_name}: Link" in content:
            return  # Already exists

        # Add TYPE_CHECKING import
        content = self._add_type_checking_import(content, target_type, target_module)

        # Generate link line
        type_hint = f"list[{target_type}]" if many else target_type
        link_line = (
            f'    {link_name}: Link[{type_hint}] = Link("{target_type}", '
            f'many={many}, reverse="{reverse}")'
        )

        # Find where to insert the link (after "# Links" comment or at end of class)
        lines = content.split("\n")
        insert_idx = None

        for i, line in enumerate(lines):
            if "# Links" in line:
                insert_idx = i + 1
                break

        if insert_idx is None:
            # Find end of class (last non-empty line before next class or EOF)
            in_class = False
            for i, line in enumerate(lines):
                if line.startswith("class "):
                    in_class = True
                elif in_class and (line.startswith("class ") or (line and not line.startswith(" ") and not line.startswith("#"))):
                    insert_idx = i
                    break
            if insert_idx is None:
                insert_idx = len(lines) - 1

        # Insert link
        lines.insert(insert_idx, link_line)
        content = "\n".join(lines)

        file_path.write_text(content, encoding="utf-8")

    def _add_type_checking_import(
        self,
        content: str,
        target_type: str,
        target_module: str,
    ) -> str:
        """Add TYPE_CHECKING import for the target type."""
        import_line = f"    from cosmos_sdk.objects.{target_module} import {target_type}"

        # Check if already imported
        if import_line in content:
            return content

        # Find TYPE_CHECKING block
        if "if TYPE_CHECKING:" in content:
            # Find the pass statement or end of TYPE_CHECKING block
            lines = content.split("\n")
            for i, line in enumerate(lines):
                if "if TYPE_CHECKING:" in line:
                    # Look for pass or existing imports
                    for j in range(i + 1, len(lines)):
                        if lines[j].strip() == "pass  # Link target imports will be added here":
                            lines[j] = import_line
                            return "\n".join(lines)
                        elif lines[j].strip() == "pass":
                            lines[j] = import_line
                            return "\n".join(lines)
                        elif not lines[j].startswith("    ") and lines[j].strip():
                            # End of TYPE_CHECKING block, insert before
                            lines.insert(j, import_line)
                            return "\n".join(lines)
                    break
        return content

    def add_link_from_dict(self, data: dict[str, Any]) -> tuple[Path, Path]:
        """
        Add link from a dictionary (e.g., from JSON).

        Args:
            data: LinkType as dictionary

        Returns:
            Tuple of (source_file, target_file) paths
        """
        link_type = LinkType.model_validate(data)
        return self.add_link(link_type)
