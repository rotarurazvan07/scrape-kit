import logging
import os
from pathlib import Path
from typing import Any

import yaml

from .errors import SettingsError

# Configure structured logging
logger = logging.getLogger("scrape_kit.settings")


class SettingsManager:
    """Recursively loads all YAML files in a directory and provides atomic writes."""

    def __init__(self, directory: str) -> None:
        self._directory = Path(directory)
        self.settings: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        """Reload all .yaml files from the directory tree into self.settings."""
        self.settings = {}
        for yaml_file in sorted(self._directory.rglob("*.yaml")):
            try:
                data = yaml.safe_load(yaml_file.read_text(encoding="utf-8")) or {}
            except yaml.YAMLError as e:
                logger.error(f"Load error for {yaml_file}: {e}")
                raise SettingsError(f"Failed to load settings file {yaml_file}: {e}") from e
            except OSError as e:
                logger.error(f"File access error for {yaml_file}: {e}")
                raise SettingsError(f"File system access failed for {yaml_file}: {e}") from e

            node = self.settings
            # relative_to gets the path from self._directory.parent
            # e.g., if self._directory is 'config', yaml_file is 'config/a/b.yaml'
            # parts will be ('config', 'a', 'b.yaml')
            for part in yaml_file.relative_to(self._directory.parent).parts[:-1]:
                node = node.setdefault(part, {})
            node[yaml_file.stem] = data

    def get(self, *keys: str) -> Any | None:
        """Fetch a value using dict paths: get('nested', 'key'). Reloads before fetch."""
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

    def write(self, directory: str, name: str, data: dict[str, Any]) -> bool:
        """Atomic write leveraging an OS-level replacement from a temp file."""
        try:
            p = Path(directory) / f"{name}.yaml"
            p.parent.mkdir(parents=True, exist_ok=True)

            temp_path = p.with_suffix(".tmp")
            temp_path.write_text(yaml.dump(data), encoding="utf-8")
            os.replace(temp_path, p)
            return True
        except (OSError, yaml.YAMLError) as e:
            logger.error(f"write failed for {name}: {e}")
            return False

    def delete(self, directory: str, name: str) -> bool:
        """Delete a YAML setting file out of the tracked directory tree."""
        try:
            p = Path(directory) / f"{name}.yaml"
            if p.exists():
                p.unlink()
            return True
        except OSError as e:
            logger.error(f"delete failed for {name}: {e}")
            return False
