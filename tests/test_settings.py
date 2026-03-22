import yaml
from settings import SettingsManager

def test_settings_manager_load(tmp_path):
    # Setup tmp config structure
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "main.yaml").write_text("key: value", encoding="utf-8")
    (tmp_path / "config" / "nested").mkdir()
    (tmp_path / "config" / "nested" / "sub.yaml").write_text("sub_key: sub_value", encoding="utf-8")

    # Instantiate with the root 'config' directory
    manager = SettingsManager(str(tmp_path / "config"))

    # Check if loaded correctly
    assert manager.get("main", "key") == "value"
    assert manager.get("nested", "sub", "sub_key") == "sub_value"
    # Test fallback search
    assert manager.get("sub_key") == "sub_value"

def test_settings_manager_write_atomic(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    manager = SettingsManager(str(config_dir))

    data = {"test_key": "test_val"}
    success = manager.write(str(config_dir), "new_cfg", data)

    assert success is True
    assert (config_dir / "new_cfg.yaml").exists()

    # Verify content
    with open(config_dir / "new_cfg.yaml", 'r', encoding="utf-8") as f:
        loaded_data = yaml.safe_load(f)
    assert loaded_data == data

def test_settings_manager_delete(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    cfg_file = config_dir / "to_delete.yaml"
    cfg_file.write_text("foo: bar", encoding="utf-8")

    manager = SettingsManager(str(config_dir))
    assert cfg_file.exists()

    success = manager.delete(str(config_dir), "to_delete")
    assert success is True
    assert not cfg_file.exists()
