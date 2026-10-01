"""SettingsManager core: init/get/write/delete."""

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from conftest import make_cfg

from scrape_kit.errors import SettingsError
from scrape_kit.settings import SettingsManager

pytestmark = pytest.mark.p0


# ── __init__ ──────────────────────────────────────────────────────────────────


class TestInit:
    """SettingsManager loads YAML trees into nested settings dict."""

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
        with patch("pathlib.Path.read_text", side_effect=OSError("Permission denied")), pytest.raises(SettingsError):
            SettingsManager(str(cfg))


# ── get ───────────────────────────────────────────────────────────────────────


class TestGet:
    """get() resolves key paths with depth-first fallback on the last key."""

    @pytest.mark.smoke
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
    """write() persists atomically and creates parent dirs."""

    def test_normal_creates_yaml_file_with_correct_content(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        data = {"host": "localhost", "port": 5432, "tls": True}
        manager.write("database", data)
        loaded = yaml.safe_load((cfg / "database.yaml").read_text(encoding="utf-8"))
        assert loaded == data

    def test_normal_atomic_write_leaves_no_temp_file(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        manager.write("atomic", {"x": 1})
        assert not (cfg / "atomic.tmp").exists()
        assert (cfg / "atomic.yaml").exists()

    def test_normal_overwrites_existing_file_with_new_data(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        (cfg / "target.yaml").write_text("old: data")
        manager = SettingsManager(str(cfg))
        manager.write("target", {"new": "data"})
        loaded = yaml.safe_load((cfg / "target.yaml").read_text(encoding="utf-8"))
        assert loaded == {"new": "data"}
        assert "old" not in loaded

    def test_edge_creates_nested_directories_automatically(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        manager.write("leaf", {"val": 42}, subpath=Path("a") / "b" / "c")
        assert Path(cfg, "a", "b", "c", "leaf.yaml").exists()

    def test_edge_write_empty_dict(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        manager.write("empty_cfg", {})
        loaded = yaml.safe_load((cfg / "empty_cfg.yaml").read_text(encoding="utf-8"))
        assert loaded is None or loaded == {}

    def test_error_write_to_path_blocked_by_file_raises(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        # A plain file sits where mkdir would need to create a directory
        blocker = tmp_path / "blocker"
        blocker.write_text("i am a file, not a dir")
        # Trying to write inside 'blocker' as if it were a directory
        with pytest.raises(SettingsError):
            manager.write("file", {"a": 1}, subpath=Path("..") / "blocker")


# ── delete ────────────────────────────────────────────────────────────────────


class TestDelete:
    """delete() removes YAML files and tolerates missing paths."""

    def test_normal_deletes_existing_file(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        target = cfg / "to_delete.yaml"
        target.write_text("x: 1")
        manager = SettingsManager(str(cfg))
        manager.delete("to_delete")
        assert not target.exists()

    def test_normal_deleted_key_no_longer_retrievable(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        (cfg / "service.yaml").write_text("url: http://example.com")
        manager = SettingsManager(str(cfg))
        assert manager.get("url") == "http://example.com"
        manager.delete("service")
        assert manager.get("url") is None

    def test_edge_delete_nonexistent_file_is_noop(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        # delete() on a missing file is a documented no-op
        assert manager.delete("ghost_file") is None
        assert not (cfg / "ghost_file.yaml").exists()

    def test_edge_delete_then_write_same_name(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        (cfg / "svc.yaml").write_text("url: old")
        manager = SettingsManager(str(cfg))
        manager.delete("svc")
        manager.write("svc", {"url": "new"})
        assert manager.get("url") == "new"

    def test_edge_delete_from_nonexistent_directory_is_noop(self, tmp_path):
        cfg = tmp_path / "config"
        cfg.mkdir()
        manager = SettingsManager(str(cfg))
        # Missing subpath is a no-op — nothing created, nothing raised
        assert manager.delete("anything", subpath="nonexistent_dir") is None
        assert not (cfg / "nonexistent_dir").exists()
