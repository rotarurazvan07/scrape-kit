import logging
import os
from pathlib import Path
from typing import Any

import yaml

from .errors import SettingsError

# Configure structured logging


# Configure structured logging
logger = logging.getLogger("scrape_kit.settings")


class SettingsManager:
    """Recursively loads all YAML files in a directory and provides atomic writes."""

    def __init__(self, directory: str) -> None:
        self._directory = Path(directory)
        self.settings: dict[str, Any] = {}
        self._load()

        logger.info("SettingsManager initialized with settings: %s", self.settings)

    def _load(self) -> None:
        """Reload all .yaml files from the directory tree into self.settings."""
        logger.debug("Reloading settings from %s...", self._directory)
        self.settings = {}

        if not self._directory.exists():
            return

        if self._directory.is_file():
            files = [self._directory]
            base_path = self._directory.parent
        else:
            files = sorted(self._directory.rglob("*.yaml"))
            base_path = self._directory.parent

        for yaml_file in files:
            logger.debug("Found config file: %s", yaml_file.name)
            try:
                data = yaml.safe_load(yaml_file.read_text(encoding="utf-8")) or {}
            except yaml.YAMLError as e:
                logger.error(f"Load error for {yaml_file}: {e}")
                raise SettingsError(f"Failed to load settings file {yaml_file}: {e}") from e
            except OSError as e:
                logger.error(f"File access error for {yaml_file}: {e}")
                raise SettingsError(f"File system access failed for {yaml_file}: {e}") from e

            node = self.settings
            try:
                # Build nested dict based on directory structure relative to parent
                for part in yaml_file.relative_to(base_path).parts[:-1]:
                    node = node.setdefault(part, {})
                node[yaml_file.stem] = data
            except ValueError:
                # If path logic fails, just put it at root
                self.settings[yaml_file.stem] = data

    def get(self, *keys: str) -> Any | None:
        """Fetch a value using dict paths: get('nested', 'key'). Reloads before fetch."""
        if not keys:
            raise SettingsError("At least one key must be provided")

        self._load()

        node = self.settings
        for key in keys:
            if not isinstance(node, dict):
                break
            node = node.get(key)
        else:
            if node is not None:
                return node

        # Fallback to a global depth-first search for the last key
        def _search(d: dict, target: str) -> Any | None:
            if target in d:
                return d[target]
            for v in d.values():
                if isinstance(v, dict):
                    result = _search(v, target)
                    if result is not None:
                        return result
            return None

        return _search(self.settings, keys[-1])

    def _resolve_target(self, name: str, subpath: str | Path | None = None) -> Path:
        base_dir = self._directory if subpath is None else self._directory / Path(subpath)
        return base_dir / f"{name}.yaml"

    def write(self, name: str, data: dict[str, Any], *, subpath: str | Path | None = None) -> None:
        """Write settings atomically under the manager's configured directory."""
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
        """Delete a YAML setting file out of the tracked directory tree."""
        try:
            p = self._resolve_target(name, subpath)
            if p.exists():
                p.unlink()
        except OSError as e:
            logger.error(f"delete failed for {name}: {e}")
            raise SettingsError(f"delete failed for {name}: {e}") from e
