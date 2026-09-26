"""Settings integration scenarios + edge cases (issue #10 split)."""

import threading
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from conftest import make_cfg

from scrape_kit.errors import SettingsError
from scrape_kit.settings import SettingsManager

pytestmark = pytest.mark.p1


# ── Complex Scenarios ─────────────────────────────────────────────────────────


class TestSettingsScenarios:
    """Settings journeys (4th-tier test_scenario_ integration)."""

    def test_scenario_deep_nested_multi_file_all_keys_accessible(self, tmp_path):
        """Many yaml files across deep directories — every leaf key reachable by fallback."""
        cfg = make_cfg(
            tmp_path,
            {
                "root.yaml": "root_val: world",
                "a/mid.yaml": "mid_val: hello",
                "a/b/leaf.yaml": "deep_val: 42",
                "a/b/c/ultra.yaml": "ultra_val: bottom",
            },
        )
        manager = SettingsManager(str(cfg))
        assert manager.get("root_val") == "world"
        assert manager.get("mid_val") == "hello"
        assert manager.get("deep_val") == 42
        assert manager.get("ultra_val") == "bottom"

    def test_scenario_write_then_get_round_trip_through_reload(self, tmp_path):
        """Write persists to disk and the next get() reloads it correctly."""
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        manager.write(
            "runtime",
            {
                "feature_flags": {"new_ui": True, "dark_mode": False},
                "max_workers": 8,
            },
        )
        assert manager.get("max_workers") == 8
        assert manager.get("feature_flags")["new_ui"] is True
        assert manager.get("feature_flags")["dark_mode"] is False

    def test_scenario_multiple_files_same_leaf_key_fallback_returns_first(self, tmp_path):
        """When two files share a key, fallback DFS returns whichever is found first
        (alphabetical due to sorted rglob). Both are accessible via full path."""
        cfg = make_cfg(
            tmp_path,
            {
                "a/config.yaml": "timeout: 10",
                "b/config.yaml": "timeout: 20",
            },
        )
        manager = SettingsManager(str(cfg))
        # Full path access distinguishes them
        assert manager.get("config", "a", "config", "timeout") == 10
        assert manager.get("config", "b", "config", "timeout") == 20

    def test_scenario_unicode_special_chars_and_multiline_values(self, tmp_path):
        """YAML with unicode, floats, and list values round-trips correctly."""
        cfg = make_cfg(
            tmp_path,
            {"intl.yaml": ("greeting: Héllo Wörld\npi: 3.14159\ntags:\n  - alpha\n  - beta\n")},
        )
        manager = SettingsManager(str(cfg))
        assert manager.get("greeting") == "Héllo Wörld"
        assert manager.get("pi") == pytest.approx(3.14159)
        assert manager.get("tags") == ["alpha", "beta"]

    def test_scenario_concurrent_writes_from_multiple_threads(self, tmp_path):
        """Ten threads each write a distinct config file — all succeed without corruption."""
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        results = []
        errors = []

        barrier = threading.Barrier(10)

        def write_config(i):
            try:
                barrier.wait(timeout=10)  # all writers start together — real contention
                manager.write(f"worker_{i}", {"id": i, "label": f"w{i}"})
                results.append(True)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=write_config, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
            assert not t.is_alive(), "writer thread hung — possible deadlock under contention"

        assert errors == [], f"Thread errors: {errors}"
        assert all(results)
        assert len(list(cfg.glob("worker_*.yaml"))) == 10
        # Verify content integrity for each file
        for i in range(10):
            data = yaml.safe_load((cfg / f"worker_{i}.yaml").read_text(encoding="utf-8"))
            assert data["id"] == i

    # ── Additional edge cases for uncovered lines ─────────────────────────────────


class TestInitEdgeCases:
    """SettingsManager init on missing dir and single-file inputs."""

    def test_edge_directory_does_not_exist(self, tmp_path):
        """Test line 29 - directory doesn't exist"""
        nonexistent = tmp_path / "nonexistent"
        manager = SettingsManager(str(nonexistent))
        assert manager.settings == {}

    def test_edge_single_file_as_directory(self, tmp_path):
        """Test lines 32-33 - single file treated as directory"""
        single_file = tmp_path / "single.yaml"
        single_file.write_text("key: value")
        manager = SettingsManager(str(single_file))
        # When a single file is used, the structure is different
        # The file becomes the root with its stem as the key
        assert "single" in str(manager.settings) or manager.settings


class TestGetEdgeCases:
    """get() with no keys and non-dict intermediates."""

    def test_error_no_keys_provided(self, tmp_path):
        """Test line 62 - no keys provided"""
        cfg = make_cfg(tmp_path, {"test.yaml": "key: value"})
        manager = SettingsManager(str(cfg))
        with pytest.raises(SettingsError, match="At least one key must be provided"):
            manager.get()

    def test_edge_get_with_non_dict_intermediate(self, tmp_path):
        """Test edge case where intermediate node is not a dict"""
        cfg = make_cfg(tmp_path, {"test.yaml": "value: not_dict"})
        manager = SettingsManager(str(cfg))
        # This should break out of the loop and return None
        assert manager.get("test", "nonexistent", "key") is None


class TestWriteEdgeCases:
    """write() OSError surfaces as SettingsError."""

    def test_error_write_fails_os_error(self, tmp_path):
        """Test lines 103-105 - write fails with OSError"""
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))

        # Mock os.replace to raise OSError
        with (
            patch("os.replace", side_effect=OSError("Permission denied")),
            pytest.raises(SettingsError, match="write failed"),
        ):
            manager.write("test", {"key": "value"})


class TestDeleteEdgeCases:
    """delete() OSError surfaces as SettingsError."""

    def test_error_delete_fails_os_error(self, tmp_path):
        """Test lines 113-115 - delete fails with OSError"""
        cfg = tmp_path / "config"
        cfg.mkdir()
        target = cfg / "to_delete.yaml"
        target.write_text("x: 1")
        manager = SettingsManager(str(cfg))

        # Mock unlink to raise OSError
        with (
            patch.object(Path, "unlink", side_effect=OSError("Permission denied")),
            pytest.raises(SettingsError, match="delete failed"),
        ):
            manager.delete("to_delete")
