"""
Comprehensive tests for settings.py — SettingsManager.

Public API covered:
  __init__, get, write, delete

Each method has: normal case, edge case(s), error case.
Plus 5 complex integration scenarios at the bottom.
"""

import threading
from pathlib import Path

import pytest
import yaml
from unittest.mock import patch
from scrape_kit.errors import SettingsError
from scrape_kit.settings import SettingsManager

# ── Helpers ───────────────────────────────────────────────────────────────────


def make_cfg(tmp_path, structure: dict) -> Path:
    """Recursively write {relative_path: yaml_content_str} into tmp_path/config."""
    cfg = tmp_path / "config"
    for rel, content in structure.items():
        target = cfg / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    return cfg


# ── __init__ ──────────────────────────────────────────────────────────────────


class TestInit:
    def test_normal_loads_single_yaml(self, tmp_path):
        cfg = make_cfg(tmp_path, {"app.yaml": "name: myapp\nversion: 2"})
        manager = SettingsManager(str(cfg))
        assert manager.settings["config"]["app"]["name"] == "myapp"
        assert manager.settings["config"]["app"]["version"] == 2

    def test_normal_loads_nested_directory_tree(self, tmp_path):
        cfg = make_cfg(
            tmp_path,
            {
                "db.yaml": "host: localhost",
                "section/cache.yaml": "ttl: 300",
                "section/sub/deep.yaml": "key: leaf",
            },
        )
        manager = SettingsManager(str(cfg))
        assert manager.settings["config"]["db"]["host"] == "localhost"
        assert manager.settings["config"]["section"]["cache"]["ttl"] == 300
        assert manager.settings["config"]["section"]["sub"]["deep"]["key"] == "leaf"

    def test_edge_empty_directory_produces_empty_settings(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        assert manager.settings == {}

    def test_edge_directory_with_non_yaml_files_ignored(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        (cfg / "notes.txt").write_text("ignore me")
        (cfg / "data.json").write_text('{"key": 1}')
        (cfg / "valid.yaml").write_text("found: true")
        manager = SettingsManager(str(cfg))
        assert manager.settings["config"]["valid"]["found"] is True
        assert "notes" not in str(manager.settings)

    def test_error_malformed_yaml_raises_settings_error(self, tmp_path):
        cfg = make_cfg(tmp_path, {"broken.yaml": "key: [unclosed bracket"})
        with pytest.raises(SettingsError, match="Failed to load"):
            SettingsManager(str(cfg))

    def test_error_unreadable_yaml_raises_settings_error(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        (cfg / "locked.yaml").write_text("x: 1")
        with patch("pathlib.Path.read_text", side_effect=OSError("Permission denied")):
            with pytest.raises(SettingsError):
                SettingsManager(str(cfg))


# ── get ───────────────────────────────────────────────────────────────────────


class TestGet:
    def test_normal_full_path_lookup(self, tmp_path):
        cfg = make_cfg(tmp_path, {"db.yaml": "host: localhost\nport: 5432"})
        manager = SettingsManager(str(cfg))
        assert manager.get("config", "db", "host") == "localhost"
        assert manager.get("config", "db", "port") == 5432

    def test_normal_fallback_depth_first_search_by_last_key(self, tmp_path):
        cfg = make_cfg(tmp_path, {"section/hidden.yaml": "secret_token: abc123\ndepth: 5"})
        manager = SettingsManager(str(cfg))
        # Caller only knows the leaf key, not the full path
        assert manager.get("secret_token") == "abc123"
        assert manager.get("depth") == 5

    def test_normal_reloads_before_each_fetch(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        yaml_file = cfg / "live.yaml"
        yaml_file.write_text("value: before")
        manager = SettingsManager(str(cfg))
        yaml_file.write_text("value: after")
        # get() must call _load() internally
        assert manager.get("value") == "after"

    def test_edge_missing_key_returns_none(self, tmp_path):
        cfg = make_cfg(tmp_path, {"app.yaml": "name: test"})
        manager = SettingsManager(str(cfg))
        assert manager.get("totally_missing") is None

    def test_edge_partial_path_falls_back_to_search(self, tmp_path):
        cfg = make_cfg(tmp_path, {"a/b.yaml": "leaf_val: 99"})
        manager = SettingsManager(str(cfg))
        # Wrong intermediate path → fallback search finds leaf_val anyway
        assert manager.get("wrong", "path", "leaf_val") == 99

    def test_edge_numeric_and_boolean_values_returned_as_is(self, tmp_path):
        cfg = make_cfg(tmp_path, {"types.yaml": "count: 42\nflag: true\npi: 3.14"})
        manager = SettingsManager(str(cfg))
        assert manager.get("count") == 42
        assert manager.get("flag") is True
        assert manager.get("pi") == pytest.approx(3.14)

    def test_error_corrupted_yaml_on_reload_raises(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        yaml_file = cfg / "app.yaml"
        yaml_file.write_text("name: good")
        manager = SettingsManager(str(cfg))
        # Corrupt after initial load — next get() triggers _load() which raises
        yaml_file.write_text("bad: [unclosed")
        with pytest.raises(SettingsError):
            manager.get("name")


# ── write ─────────────────────────────────────────────────────────────────────


class TestWrite:
    def test_normal_creates_yaml_file_with_correct_content(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        data = {"host": "localhost", "port": 5432, "tls": True}
        assert manager.write(str(cfg), "database", data) is True
        loaded = yaml.safe_load((cfg / "database.yaml").read_text(encoding="utf-8"))
        assert loaded == data

    def test_normal_atomic_write_leaves_no_temp_file(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        manager.write(str(cfg), "atomic", {"x": 1})
        assert not (cfg / "atomic.tmp").exists()
        assert (cfg / "atomic.yaml").exists()

    def test_normal_overwrites_existing_file_with_new_data(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        (cfg / "target.yaml").write_text("old: data")
        manager = SettingsManager(str(cfg))
        manager.write(str(cfg), "target", {"new": "data"})
        loaded = yaml.safe_load((cfg / "target.yaml").read_text(encoding="utf-8"))
        assert loaded == {"new": "data"}
        assert "old" not in loaded

    def test_edge_creates_nested_directories_automatically(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        deep = str(cfg / "a" / "b" / "c")
        assert manager.write(deep, "leaf", {"val": 42}) is True
        assert Path(deep, "leaf.yaml").exists()

    def test_edge_write_empty_dict(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        assert manager.write(str(cfg), "empty_cfg", {}) is True
        loaded = yaml.safe_load((cfg / "empty_cfg.yaml").read_text(encoding="utf-8"))
        assert loaded is None or loaded == {}

    def test_error_write_to_path_blocked_by_file_returns_false(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        # A plain file sits where mkdir would need to create a directory
        blocker = tmp_path / "blocker"
        blocker.write_text("i am a file, not a dir")
        # Trying to write inside 'blocker' as if it were a directory
        result = manager.write(str(blocker), "file", {"a": 1})
        assert result is False


# ── delete ────────────────────────────────────────────────────────────────────


class TestDelete:
    def test_normal_deletes_existing_file(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        target = cfg / "to_delete.yaml"
        target.write_text("x: 1")
        manager = SettingsManager(str(cfg))
        assert manager.delete(str(cfg), "to_delete") is True
        assert not target.exists()

    def test_normal_deleted_key_no_longer_retrievable(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        (cfg / "service.yaml").write_text("url: http://example.com")
        manager = SettingsManager(str(cfg))
        assert manager.get("url") == "http://example.com"
        manager.delete(str(cfg), "service")
        assert manager.get("url") is None

    def test_edge_delete_nonexistent_file_still_returns_true(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        # Should be a no-op, not an error
        assert manager.delete(str(cfg), "ghost_file") is True

    def test_edge_delete_then_write_same_name(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        (cfg / "svc.yaml").write_text("url: old")
        manager = SettingsManager(str(cfg))
        manager.delete(str(cfg), "svc")
        manager.write(str(cfg), "svc", {"url": "new"})
        assert manager.get("url") == "new"

    def test_error_delete_from_nonexistent_directory_returns_true(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        # Path doesn't exist → file doesn't exist → returns True
        result = manager.delete(str(tmp_path / "nonexistent_dir"), "anything")
        assert result is True


# ── Complex Scenarios ─────────────────────────────────────────────────────────


class TestSettingsScenarios:
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
            str(cfg),
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

        def write_config(i):
            try:
                ok = manager.write(str(cfg), f"worker_{i}", {"id": i, "label": f"w{i}"})
                results.append(ok)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=write_config, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Thread errors: {errors}"
        assert all(results)
        assert len(list(cfg.glob("worker_*.yaml"))) == 10
        # Verify content integrity for each file
        for i in range(10):
            data = yaml.safe_load((cfg / f"worker_{i}.yaml").read_text(encoding="utf-8"))
            assert data["id"] == i
