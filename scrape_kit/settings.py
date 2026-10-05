"""YAML settings management: SettingsManager with atomic writes and depth-first key lookup."""

import os
from pathlib import Path
from typing import Any, Final

import yaml  # type: ignore[import-untyped]  # PyYAML 6.0.3 ships no py.typed marker (no stubs); revisit if types-PyYAML lands

from .errors import SettingsError
from .logger import get_logger

logger = get_logger(__name__)


_NOT_FOUND: Final = object()


def _depth_first_search(data: dict[str, Any], target: str) -> Any:
    """Search a nested mapping depth-first for a target key.

    A key that is present with a ``None`` value counts as found; the search
    stops at the first hit instead of skipping past it.

    Args:
        data: Nested mapping to search.
        target: Key to look for at any nesting depth.

    Returns:
        The value for the target key if found, the ``_NOT_FOUND`` sentinel otherwise.
    """
    if target in data:
        return data[target]
    for value in data.values():
        if isinstance(value, dict):
            result = _depth_first_search(value, target)
            if result is not _NOT_FOUND:
                return result
    return _NOT_FOUND


class SettingsManager:
    """Recursively loads all YAML files in a directory and provides atomic writes."""

    def __init__(self, directory: str) -> None:
        """Initialize the SettingsManager.

        Args:
            directory: Path to the directory containing YAML configuration files.
        """
        self._directory = Path(directory)
        self.settings: dict[str, Any] = {}
        self._load()

        logger.info(
            "SettingsManager initialized from %s with %d top-level keys",
            self._directory,
            len(self.settings),
        )

    def _load(self) -> None:
        """Reload all YAML files from the directory tree into self.settings.

        Supports both a single file or a directory tree. Builds nested dictionaries
        based on the directory structure relative to the parent of the config directory.

        Raises:
            SettingsError: If any YAML file is malformed/unreadable, or if the
                layout is ambiguous (a YAML stem collides with a sibling directory).
        """
        logger.debug("Reloading settings from %s...", self._directory)
        self.settings = {}

        if not self._directory.exists():
            return

        files = [self._directory] if self._directory.is_file() else sorted(self._directory.rglob("*.yaml"))
        base_path = self._directory.parent

        # Explicit-errors philosophy: a stem colliding with a sibling directory
        # (db.yaml + db/) would silently lose one side's data to the last write —
        # detect the layout ambiguity up front instead.
        if len(files) > 1:
            stems = {f.stem for f in files}
            dirs = {p.relative_to(self._directory).parts[0] for p in files if len(p.relative_to(self._directory).parts) >= 2}
            ambiguous = stems & dirs
            if ambiguous:
                raise SettingsError(
                    f"Ambiguous config layout: both a .yaml file and a directory provide key(s) {sorted(ambiguous)}"
                )

        for yaml_file in files:
            logger.debug("Found config file: %s", yaml_file.name)
            self._nest(yaml_file, base_path, self._read_yaml(yaml_file))

    def _read_yaml(self, yaml_file: Path) -> dict[str, Any]:
        """Load one YAML file into a dict, mapping failures to SettingsError.

        Args:
            yaml_file: The file to read.

        Returns:
            The parsed mapping (empty dict for an empty file).

        Raises:
            SettingsError: If the file is malformed or unreadable.
        """
        try:
            return yaml.safe_load(yaml_file.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as e:
            logger.error("Load error for %s: %s", yaml_file, e)
            raise SettingsError(f"Failed to load settings file {yaml_file}: {e}") from e
        except OSError as e:
            logger.error("File access error for %s: %s", yaml_file, e)
            raise SettingsError(f"File system access failed for {yaml_file}: {e}") from e

    def _nest(self, yaml_file: Path, base_path: Path, data: dict[str, Any]) -> None:
        """Merge one loaded YAML file into ``self.settings`` at its nested location.

        The file's directory structure relative to ``base_path`` becomes nested
        dict keys; if the path logic fails, the data lands at the root instead.

        Args:
            yaml_file: The file the data came from.
            base_path: The parent directory the relative structure is built from.
            data: The already-loaded mapping to place.
        """
        node = self.settings
        try:
            for part in yaml_file.relative_to(base_path).parts[:-1]:
                node = node.setdefault(part, {})
            node[yaml_file.stem] = data
        except ValueError:
            self.settings[yaml_file.stem] = data

    def get(self, *keys: str) -> Any | None:
        """Fetch a configuration value using a path of keys.

        The method reloads settings before each fetch to ensure fresh data.
        A key that is present with a ``None`` value counts as found and
        returns ``None``; only an absent key falls back to a depth-first
        search for the last key (and to ``None`` if that also misses).

        Args:
            *keys: A sequence of keys representing the path to the value.

        Returns:
            The value if found, None otherwise.

        Raises:
            SettingsError: If no keys are provided, or any YAML file fails to load.
        """
        if not keys:
            raise SettingsError("At least one key must be provided")

        self._load()

        node: Any = self.settings
        for key in keys:
            if not isinstance(node, dict) or key not in node:
                break
            node = node[key]
        else:
            return node

        # Fallback to a global depth-first search for the last key
        result = _depth_first_search(self.settings, keys[-1])
        return None if result is _NOT_FOUND else result

    def _resolve_target(self, name: str, subpath: str | Path | None = None) -> Path:
        """Resolve the full path for a settings file, enforcing containment.

        The resolved path must stay inside the settings directory; caller-supplied
        names or subpaths that would escape it raise ``SettingsError``.

        Args:
            name: The base name of the settings file (without extension).
            subpath: Optional subdirectory within the config directory.

        Returns:
            The full Path to the YAML file.

        Raises:
            SettingsError: If path resolution fails or the target escapes the
                settings directory.
        """
        base_dir = self._directory if subpath is None else self._directory / Path(subpath)
        target = base_dir / f"{name}.yaml"
        try:
            resolved_root = self._directory.resolve()
            resolved_target = target.resolve()
        except OSError as e:
            raise SettingsError(f"Cannot resolve settings path {target}: {e}") from e
        if not resolved_target.is_relative_to(resolved_root):
            raise SettingsError(f"Settings path {target} must stay within the settings directory {self._directory}")
        return target

    def write(self, name: str, data: dict[str, Any], *, subpath: str | Path | None = None) -> None:
        """Write settings atomically to a YAML file.

        Uses a temporary file and atomic rename to prevent corruption.

        Args:
            name: The base name of the settings file (without extension).
            data: The dictionary data to write as YAML.
            subpath: Optional subdirectory within the config directory.

        Raises:
            SettingsError: If writing fails due to I/O or YAML errors.
        """
        try:
            p = self._resolve_target(name, subpath)
            logger.info("Writing settings to %s...", p)
            p.parent.mkdir(parents=True, exist_ok=True)

            temp_path = p.with_suffix(".tmp")
            temp_path.write_text(yaml.dump(data), encoding="utf-8")
            os.replace(temp_path, p)
            logger.debug("Write successful for %s", name)
        except (OSError, yaml.YAMLError) as e:
            logger.error(f"write failed for {name}: {e}")
            raise SettingsError(f"write failed for {name}: {e}") from e

    def delete(self, name: str, *, subpath: str | Path | None = None) -> None:
        """Delete a YAML settings file.

        Args:
            name: The base name of the settings file (without extension).
            subpath: Optional subdirectory within the config directory.
        """
        try:
            p = self._resolve_target(name, subpath)
            if p.exists():
                p.unlink()
        except OSError as e:
            logger.error(f"delete failed for {name}: {e}")
            raise SettingsError(f"delete failed for {name}: {e}") from e
