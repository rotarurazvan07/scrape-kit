"""retry_hook / max_retries on scrape(): gating, redelivery, exhaustion policy."""

import logging
from unittest.mock import MagicMock, patch

import pytest

import scrape_kit
import scrape_kit.fetcher as fetcher_module
from scrape_kit.errors import FetcherError
from scrape_kit.fetcher import RetryHook
from scrape_kit.fetcher.batch_types import _UserCodeError
from scrape_kit.fetcher.engine_adapter import ScraplingEngineAdapter
from scrape_kit.fetcher.web_fetcher import WebFetcher

pytestmark = pytest.mark.p0

A = "http://a.com"
B = "http://b.com"
C = "http://c.com"
GHOST = "http://ghost.com"


def _flaky_fetch(bad):
    """Mock fetch: the bad URL raises, others return clean HTML."""

    def _fetch(url, **_kwargs):
        if url == bad:
            raise RuntimeError("boom")
        return f"<html>ok-{url}</html>"

    return MagicMock(side_effect=_fetch)


def _reason_hook(**reasons):
    """Hook rejecting the URLs named in reasons; records every call."""
    calls = []

    def _hook(url, html):
        reason = reasons.get(url)
        calls.append((url, reason or ""))
        return reason

    _hook.calls = calls
    return _hook


class _RecordingSpider:
    """Spider stand-in that replays its start_urls through the callback."""

    def __init__(self):
        self.start_urls = ()
        self.started = 0
        self._callback = lambda url, html, final=False: None
        self._errors_urls: list[str] = []
        self._final = False

    def start(self):
        self.started += 1
        self._errors_urls = []
        for url in list(self.start_urls):
            try:
                self._callback(url, f"<html>page-{url}</html>", self._final)
            except _UserCodeError:
                raise
            except (FetcherError, RuntimeError):
                self._errors_urls.append(url)
        if self._errors_urls:
            raise RuntimeError(", ".join(self._errors_urls))


def _spider_builder(spider):
    """Return a _build_spider replacement that wires the spider per pass.

    A class-level patch receives the call arguments without a bound self, so
    the first two positional arguments are the URL list and the callback.
    """

    def _build(*args):
        spider.start_urls = tuple(args[0])
        spider._callback = args[1]
        return spider

    return _build


class TestNoHookPathUnchanged:
    """Without retry_hook/max_retries the delivery behavior is unchanged."""

    @patch.object(WebFetcher, "fetch")
    def test_normal_fast_scrape_without_hook_delivers_every_url(self, mock_fetch):
        mock_fetch.side_effect = lambda url, **kw: f"<html>{url}</html>"
        fetcher = WebFetcher()
        results = []
        fetcher.scrape([A, B, C], callback=lambda url, html: results.append(url))
        assert sorted(results) == [A, B, C]

    @patch.object(WebFetcher, "fetch")
    def test_edge_hook_without_max_retries_is_inert(self, mock_fetch):
        mock_fetch.side_effect = lambda url, **kw: f"<html>{url}</html>"
        fetcher = WebFetcher()
        hook = MagicMock(return_value=None)
        delivered = []
        fetcher.scrape([A], callback=lambda url, html: delivered.append(url), retry_hook=hook, max_retries=0)
        hook.assert_not_called()
        assert delivered == [A]

    def test_error_invalid_mode_still_raises_when_hook_is_configured(self):
        fetcher = WebFetcher()
        hook = MagicMock(return_value="reason")
        with pytest.raises(ValueError, match="Unsupported scrape mode"):
            fetcher.scrape([A], callback=MagicMock(), mode="invalid", retry_hook=hook, max_retries=2)
        hook.assert_not_called()

    def test_error_max_retries_without_hook_raises_value_error(self):
        fetcher = WebFetcher()
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            fetcher.scrape([A], callback=MagicMock(), max_retries=-1)


class TestHookVerdicts:
    """Hook accept delivers, hook reject defers, hook exceptions propagate."""

    @patch.object(WebFetcher, "fetch")
    def test_normal_hook_accept_delivers_url_once(self, mock_fetch):
        mock_fetch.return_value = "<html>Clean</html>"
        fetcher = WebFetcher()
        results = []
        fetcher.scrape(
            [A],
            callback=lambda url, html: results.append(url),
            retry_hook=lambda url, html: None,
            max_retries=2,
        )
        assert results == [A]
        assert mock_fetch.call_count == 1

    def test_normal_hook_reject_defers_url_to_redelivery(self, caplog):
        fetcher = WebFetcher()
        hook = _reason_hook(**{A: "reason-a"})
        fetch_mock = _flaky_fetch(GHOST)
        with (
            caplog.at_level(logging.WARNING, logger="scrape_kit.fetcher.engine_adapter"),
            patch.object(fetcher, "fetch", fetch_mock),
            pytest.raises(FetcherError, match="FAST mode fetch failed"),
        ):
            fetcher.scrape([A, GHOST], callback=lambda url, html: None, retry_hook=hook, max_retries=1)
        urls = [c.args[0] for c in fetch_mock.call_args_list]
        assert urls.count(A) == 2
        assert urls.count(GHOST) == 2
        assert [reason for url, reason in hook.calls if url == A] == ["reason-a", "reason-a"]

    @patch.object(WebFetcher, "fetch")
    def test_error_hook_exception_propagates(self, mock_fetch):
        mock_fetch.return_value = "<html>Clean</html>"
        fetcher = WebFetcher()

        def bad_hook(url, html):
            raise RuntimeError("hook bug")

        with pytest.raises(RuntimeError, match="hook bug"):
            fetcher.scrape([A], callback=MagicMock(), retry_hook=bad_hook, max_retries=3)
        assert mock_fetch.call_count == 1


class TestExhaustionPolicy:
    """Final pass delivers a persistently rejected page once and warns."""

    @patch.object(WebFetcher, "fetch")
    def test_normal_persistent_hook_rejection_delivers_with_warning(self, mock_fetch, caplog):
        mock_fetch.return_value = "<html>reject</html>"
        fetcher = WebFetcher()
        hook = _reason_hook(**{A: "thin-content"})
        with caplog.at_level(logging.WARNING, logger="scrape_kit.fetcher.engine_adapter"):
            delivered = []
            fetcher.scrape(
                [A],
                callback=lambda url, html: delivered.append(url),
                retry_hook=hook,
                max_retries=2,
            )
        assert delivered == [A]
        assert [reason for _, reason in hook.calls] == ["thin-content"] * 3
        fetched = [c.args[0] for c in mock_fetch.call_args_list]
        assert fetched == [A, A, A]
        assert any("thin-content" in record.message and A in record.message for record in caplog.records)


class TestStealthParity:
    """The same gating semantics hold when mode=stealth drives the engine."""

    @patch.object(ScraplingEngineAdapter, "_build_spider")
    def test_normal_stealth_hook_rejection_reenters_engine(self, mock_build_spider):
        spider = _RecordingSpider()
        mock_build_spider.side_effect = _spider_builder(spider)
        fetcher = WebFetcher()
        states = iter(["reason-a", "reason-a", "accepted"])
        seen = []

        def bad(url, html):
            verdict = next(states)
            seen.append(verdict)
            return None if verdict == "accepted" else verdict

        delivered = []
        fetcher.scrape(
            [A],
            callback=lambda url, html: delivered.append(url),
            mode="stealth",
            retry_hook=bad,
            max_retries=2,
            max_concurrency=1,
        )
        assert delivered == [A]
        assert seen == ["reason-a", "reason-a", "accepted"]
        assert spider.started == 3

    @patch.object(ScraplingEngineAdapter, "_build_spider")
    def test_error_stealth_final_attempt_raises(self, mock_build_spider):
        spider = MagicMock()
        spider.start.side_effect = RuntimeError("page crashed")
        spider.start_urls = (A,)
        spider._errors_urls = []
        mock_build_spider.return_value = spider
        fetcher = WebFetcher()
        with pytest.raises(FetcherError, match="Scrape failed"):
            fetcher.scrape(
                [A],
                callback=lambda url, html: None,
                mode="stealth",
                retry_hook=lambda url, html: "reason-a",
                max_retries=2,
                max_concurrency=1,
            )
        assert spider.start.call_count == 1


class TestGuardsAndConcurrency:
    """Batches with mixed verdicts deliver every URL exactly once."""

    @patch.object(WebFetcher, "fetch")
    def test_normal_fast_mixed_verdicts_are_exact(self, mock_fetch):
        fetcher = WebFetcher()
        hook = _reason_hook(**{A: "reason-a", C: "reason-c"})

        def _fetch(url, **_kwargs):
            return f"<html>{url}</html>"

        fetch_mock = MagicMock(side_effect=_fetch)
        with patch.object(fetcher, "fetch", fetch_mock):
            delivered = []
            fetcher.scrape(
                [A, B, C, GHOST],
                callback=lambda url, html: delivered.append(url),
                retry_hook=hook,
                max_retries=1,
            )
        assert len(delivered) == 4
        for url in (A, B, C, GHOST):
            assert delivered.count(url) == 1
        fetched_order = [c.args[0] for c in fetch_mock.call_args_list]
        assert sorted(fetched_order[:4]) == sorted([A, B, C, GHOST])
        assert sorted(fetched_order[4:]) == sorted([A, C])
        assert [reason for url, reason in hook.calls if url == A] == ["reason-a", "reason-a"]
        assert [reason for url, reason in hook.calls if url == B] == [""]

    def test_edge_empty_urls_with_hook_returns_immediately(self):
        fetcher = WebFetcher()
        hook = MagicMock(return_value="reason")
        delivered = []
        fetcher.scrape([], callback=lambda url, html: delivered.append(url), retry_hook=hook, max_retries=3)
        assert delivered == []
        hook.assert_not_called()

    def test_error_negative_max_retries_with_hook_raises(self):
        fetcher = WebFetcher()
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            fetcher.scrape([A], callback=MagicMock(), retry_hook=lambda url, html: "r", max_retries=-2)

    @patch.object(WebFetcher, "fetch")
    def test_error_mid_retry_final_attempt_raises(self, mock_fetch):
        fetcher = WebFetcher()
        hook = _reason_hook(**{A: "reason-a"})
        failing = _flaky_fetch(GHOST)
        with patch.object(fetcher, "fetch", failing), pytest.raises(FetcherError, match="FAST mode fetch failed"):
            fetcher.scrape([A, GHOST], callback=lambda url, html: None, retry_hook=hook, max_retries=1)
        fetched = [c.args[0] for c in failing.call_args_list]
        assert sorted(fetched) == sorted([A, GHOST, A, GHOST])
        assert len(fetched) == 4
        assert [reason for _, reason in hook.calls] == ["reason-a", "reason-a"]


class TestProxyForwarding:
    """The module-level proxies accept and forward the new keyword arguments."""

    def test_normal_module_scrape_proxy_forwards_kwargs(self):
        fetcher_module.reset_shared()
        with patch.object(WebFetcher, "scrape", autospec=True) as mock_scrape:
            fetcher_module.scrape([A], callback=MagicMock(), retry_hook=lambda url, html: None, max_retries=3)
            mock_scrape.assert_called_once()
            assert mock_scrape.call_args.kwargs["retry_hook"] is not None
            assert mock_scrape.call_args.kwargs["max_retries"] == 3

    def test_normal_package_scrape_proxy_forwards_kwargs(self):
        fetcher_module.reset_shared()
        with patch.object(WebFetcher, "scrape", autospec=True) as mock_scrape:
            scrape_kit.scrape([A], callback=MagicMock(), retry_hook=lambda url, html: None, max_retries=2)
            assert mock_scrape.call_args.kwargs["max_retries"] == 2

    def test_normal_retry_hook_alias_is_the_documented_type(self):
        assert callable(RetryHook)
        assert scrape_kit.RetryHook is fetcher_module.RetryHook


class TestExistingLayersStillApply:
    """FAST-mode fetches keep their internal retry and escalation policy."""

    @patch.object(WebFetcher, "fetch")
    def test_normal_inner_retry_loop_still_runs_under_hook(self, mock_fetch):
        mock_fetch.side_effect = ["<html>blocked</html>", "<html>Clean</html>"]
        fetcher = WebFetcher(block_indicators=["blocked"])
        hook = MagicMock(return_value=None)
        results = []
        fetcher.scrape(
            [A],
            callback=lambda url, html: results.append(url),
            retry_hook=hook,
            max_retries=1,
        )
        assert results == [A]
        assert [c.args[0] for c in hook.call_args_list] == [A]

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep", return_value=None)
    @patch.object(WebFetcher, "_retry_wait", return_value=0.0)
    @patch.object(WebFetcher, "browser")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher.get")
    def test_normal_escalation_serves_the_hook_the_escalated_html(self, mock_get, mock_browser, mock_wait, mock_sleep):
        fetcher = WebFetcher(retry_indicators=["Just a moment"])
        mock_get.return_value = MagicMock(status=200, html_content="<html>Just a moment</html>")

        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock()
        session.fetch.return_value = "<html>escalated-via-browser</html>"
        session._last_response_status = MagicMock(return_value=200)
        session._last_retry_after = MagicMock(return_value=None)
        mock_browser.return_value = session

        hook = MagicMock(return_value=None)
        results = []
        fetcher.scrape(
            [A],
            callback=lambda url, html: results.append(url),
            retry_hook=hook,
            max_retries=1,
        )
        assert mock_browser.call_count == 1
        assert results == [A]
        assert [c.args[0] for c in hook.call_args_list] == [A]


class TestUserCodeFailureContract:
    """Hook/callback FetcherError escapes aggregation and the retry loop."""

    @patch.object(WebFetcher, "fetch")
    def test_error_hook_raising_fetcher_error_propagates_immediately(self, mock_fetch):
        mock_fetch.return_value = "<html>Clean</html>"
        fetcher = WebFetcher()

        def bad_hook(url, html):
            raise FetcherError("hook-domain-failure")

        with pytest.raises(_UserCodeError) as exc_info:
            fetcher.scrape([A], callback=MagicMock(), retry_hook=bad_hook, max_retries=3)
        assert isinstance(exc_info.value.__cause__, FetcherError)
        assert mock_fetch.call_count == 1

    @patch.object(WebFetcher, "fetch")
    def test_error_callback_raising_fetcher_error_propagates_immediately(self, mock_fetch):
        mock_fetch.return_value = "<html>Clean</html>"
        fetcher = WebFetcher()
        calls = []

        def bad_callback(url, html):
            calls.append(url)
            raise FetcherError("callback-domain-failure")

        with pytest.raises(_UserCodeError) as exc_info:
            fetcher.scrape([A], callback=bad_callback, retry_hook=lambda url, html: None, max_retries=2)
        assert calls == [A]
        assert mock_fetch.call_count == 1
        assert isinstance(exc_info.value.__cause__, FetcherError)

    @patch.object(ScraplingEngineAdapter, "_build_spider")
    def test_error_stealth_hook_raising_fetcher_error_propagates(self, mock_build_spider):
        spider = _RecordingSpider()
        mock_build_spider.side_effect = _spider_builder(spider)
        fetcher = WebFetcher()

        def bad_hook(url, html):
            raise FetcherError("hook-domain-failure")

        with pytest.raises(_UserCodeError) as exc_info:
            fetcher.scrape(
                [A],
                callback=MagicMock(),
                mode="stealth",
                retry_hook=bad_hook,
                max_retries=2,
            )
        assert isinstance(exc_info.value.__cause__, FetcherError)
        assert spider.started == 1
        assert spider._errors_urls == []


class TestDuplicateUrlBookkeeping:
    """Duplicate URLs each keep their own retry and delivery."""

    @patch.object(WebFetcher, "fetch")
    def test_normal_duplicate_urls_each_delivered(self, mock_fetch):
        fetcher = WebFetcher()
        seen = []

        def _fetch(url, **_kwargs):
            seen.append(url)
            if url == A and seen.count(A) <= 2:
                raise RuntimeError("transient")
            return f"<html>ok-{url}</html>"

        fetch_mock = MagicMock(side_effect=_fetch)
        with patch.object(fetcher, "fetch", fetch_mock):
            delivered = []
            fetcher.scrape(
                [A, A],
                callback=lambda url, html: delivered.append(url),
                retry_hook=lambda url, html: None,
                max_retries=1,
            )
        assert delivered == [A, A]

    @patch.object(WebFetcher, "fetch")
    def test_error_max_concurrency_zero_with_hook_raises_before_fetch(self, mock_fetch):
        fetcher = WebFetcher()
        hook = MagicMock(return_value=None)
        with pytest.raises(ValueError, match="max_concurrency must be >= 1"):
            fetcher.scrape([A], callback=MagicMock(), retry_hook=hook, max_retries=1, max_concurrency=0)
        mock_fetch.assert_not_called()
        hook.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
