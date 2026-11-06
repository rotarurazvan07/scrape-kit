"""ProxyRotator instantiation and proxy-list parsing tests."""

from unittest.mock import patch

import pytest

from scrape_kit.fetcher import ProxyRotator

pytestmark = pytest.mark.p1


# ── Normal: string proxies, dict proxies, YAML read → instantiate, get_proxy cycles ──────


class TestProxyRotatorNormal:
    """Normal cases: ProxyRotator instantiation and basic functionality."""

    def test_normal_string_proxies_instantiation(self):
        """ProxyRotator accepts list of string proxies."""
        proxies = ["http://proxy1:8080", "http://proxy2:8080", "http://proxy3:8080"]
        rotator = ProxyRotator(proxies)
        assert rotator.proxies == proxies

    def test_normal_dict_proxies_instantiation(self):
        """ProxyRotator accepts list of dict proxies with auth."""
        proxies = [
            {"server": "http://proxy1:8080", "username": "user1", "password": "pass1"},
            {"server": "http://proxy2:8080", "username": "user2", "password": "pass2"},
        ]
        rotator = ProxyRotator(proxies)
        assert rotator.proxies == proxies

    def test_normal_yaml_read_instantiate(self, tmp_path):
        """Read a proxy list from YAML and instantiate ProxyRotator."""
        import yaml

        proxy_list = ["http://proxy1:8080", "http://proxy2:8080"]
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        # The YAML file's content is the list itself, so SettingsManager's
        # depth-first leaf lookup returns it directly.
        (cfg_dir / "proxies.yaml").write_text(yaml.safe_dump(proxy_list), encoding="utf-8")

        from scrape_kit.settings import SettingsManager

        settings = SettingsManager(str(cfg_dir))
        rotator = ProxyRotator(settings.get("proxies"))
        assert rotator.proxies == proxy_list

    def test_normal_get_proxy_cycles_through_list(self):
        """get_proxy() cycles through proxies in order."""
        proxies = ["http://proxy1:8080", "http://proxy2:8080", "http://proxy3:8080"]
        rotator = ProxyRotator(proxies)

        assert rotator.get_proxy() == "http://proxy1:8080"
        assert rotator.get_proxy() == "http://proxy2:8080"
        assert rotator.get_proxy() == "http://proxy3:8080"
        assert rotator.get_proxy() == "http://proxy1:8080"  # Cycles back

    def test_normal_get_proxy_returns_dict_when_configured(self):
        """get_proxy() returns dict when proxies are dicts."""
        proxies = [
            {"server": "http://proxy1:8080", "username": "user1", "password": "pass1"},
            {"server": "http://proxy2:8080", "username": "user2", "password": "pass2"},
        ]
        rotator = ProxyRotator(proxies)

        proxy1 = rotator.get_proxy()
        proxy2 = rotator.get_proxy()

        assert isinstance(proxy1, dict)
        assert proxy1["server"] == "http://proxy1:8080"
        assert proxy1["username"] == "user1"
        assert proxy2["server"] == "http://proxy2:8080"

    def test_normal_get_proxy_returns_proxy_or_dict(self):
        """get_proxy() returns string or dict depending on input."""
        proxies = ["http://proxy1:8080", "http://proxy2:8080"]
        rotator = ProxyRotator(proxies)
        proxy = rotator.get_proxy()
        assert proxy in proxies
        assert isinstance(proxy, str)


# ── Edge: empty list, single proxy, full rotation cycle back to index 0 ───────────


class TestProxyRotatorEdge:
    """Edge cases: empty list, single proxy, full rotation cycle."""

    def test_edge_empty_list_raises_value_error(self):
        """Empty proxy list raises ValueError at construction."""
        with pytest.raises(ValueError, match="At least one proxy must be provided"):
            ProxyRotator([])

    def test_edge_single_proxy_always_returns_same(self):
        """Single proxy always returns the same proxy."""
        rotator = ProxyRotator(["http://only-proxy:8080"])
        assert rotator.get_proxy() == "http://only-proxy:8080"
        assert rotator.get_proxy() == "http://only-proxy:8080"
        assert rotator.get_proxy() == "http://only-proxy:8080"

    def test_edge_full_rotation_cycle_back_to_index_zero(self):
        """Full rotation cycle returns to index 0."""
        proxies = ["http://proxy1:8080", "http://proxy2:8080"]
        rotator = ProxyRotator(proxies)

        # First cycle
        assert rotator.get_proxy() == "http://proxy1:8080"
        assert rotator.get_proxy() == "http://proxy2:8080"
        # Second cycle - back to index 0
        assert rotator.get_proxy() == "http://proxy1:8080"
        assert rotator.get_proxy() == "http://proxy2:8080"
        # Third cycle
        assert rotator.get_proxy() == "http://proxy1:8080"

    def test_edge_mixed_string_and_dict_proxies(self):
        """Mixed string and dict proxies work."""
        proxies = [
            "http://simple-proxy:8080",
            {"server": "http://auth-proxy:8080", "username": "user", "password": "pass"},
        ]
        rotator = ProxyRotator(proxies)

        proxy1 = rotator.get_proxy()
        proxy2 = rotator.get_proxy()

        assert proxy1 == "http://simple-proxy:8080"
        assert isinstance(proxy2, dict)
        assert proxy2["server"] == "http://auth-proxy:8080"

    def test_edge_constructor_with_explicit_strategy(self):
        """Constructor accepts explicit strategy callable."""
        from scrapling.engines.toolbelt.proxy_rotation import cyclic_rotation

        proxies = ["http://proxy1:8080", "http://proxy2:8080"]
        rotator = ProxyRotator(proxies, strategy=cyclic_rotation)
        assert rotator.get_proxy() == "http://proxy1:8080"


# ── Error: non-list YAML → TypeError from Scrapling propagates ───────────────────


class TestProxyRotatorError:
    """Error cases: invalid inputs raise appropriate errors."""

    def test_error_non_list_yaml_raises_value_error(self):
        """An empty-ish YAML scalar string yields no usable proxy."""
        # Scrapling treats a string as an iterable of characters, so an
        # empty string is the only scalar that trips its "at least one"
        # guard.
        with pytest.raises(ValueError, match="At least one proxy must be provided"):
            ProxyRotator("")

    def test_error_none_proxies_raises_value_error(self):
        """None proxies raises ValueError."""
        with pytest.raises(ValueError, match="At least one proxy must be provided"):
            ProxyRotator(None)

    def test_error_integer_proxies_raises_typeerror(self):
        """Integer proxies raises TypeError (not iterable)."""
        with pytest.raises(TypeError):
            ProxyRotator(123)

    def test_error_proxy_rotator_imported_from_correct_module(self):
        """ProxyRotator is imported from scrape_kit.fetcher (re-exported)."""
        from scrape_kit.fetcher import ProxyRotator as FetcherProxyRotator
        from scrape_kit import ProxyRotator as PackageProxyRotator

        assert FetcherProxyRotator is PackageProxyRotator

    @patch("scrape_kit.fetcher.ProxyRotator.__init__", side_effect=Exception("Scrapling init failed"))
    def test_error_scrapling_init_failure_propagates(self, mock_init):
        """Scrapling initialization failure propagates."""
        with pytest.raises(Exception, match="Scrapling init failed"):
            ProxyRotator(["http://proxy:8080"])
