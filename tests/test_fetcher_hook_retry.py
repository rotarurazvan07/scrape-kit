"""retry_hook / max_retries on scrape(): hook gating, redelivery safety, exhaustion policy."""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from conftest import make_page

import scrape_kit
import scrape_kit.fetcher as fetcher_module
from scrape_kit.errors import FetcherError
from scrape_kit.fetcher import RetryHook
from scrape_kit.fetcher.web_fetcher import WebFetcher

pytestmark = pytest.mark.p0

A = "http://a.com"
B = "http://b.com"
C = "http://c.com"
GHOST = "http://ghost.com"


# ── shared helpers ────────────────────────────────────────────────────────────


def _flaky_fetch(bad: str) -> MagicMock:
    """Mock fetch: ``bad`` URL raises, others return clean HTML."""

    def _fetch(url: str, **_kwargs: str) -> str:
        if url == bad:
            raise RuntimeError("boom")
        return f"<html>ok-{url}</html>"

    return MagicMock(side_effect=_fetch)


def _reason_hook(**reasons: str) -> list[tuple[str, str]]:
    """Hook rejecting the URLs named in ``reasons``; records every call.

    Args:
        **reasons: Mapping of URL to the reason returned when it is checked.

    Returns:
        The hook callable; it stores ``(url, reason)`` pairs in a list.
    """
    calls: list[tuple[str, str]] = []

    def _hook(url: str, html: str) -> str | None:
        reason = reasons.get(url)
        calls.append((url, reason or ""))
        return reason

    _hook.calls = calls  # type: ignore[attr-defined]
    return _hook  # type: ignore[return-value]


def _stealth_page(url: str, attempts: dict[str, int], html: str | None = None) -> MagicMock:
    """Build a stealth page mock that serves fresh HTML per call."""
    page = MagicMock()
    attempt = attempts.get(url, 0) + 1
    attempts[url] = attempt
    page.status = 200
    page.html_content = html if html is not None else f"<html>{url}#{attempt}</html>"
    return page


def _stealth_cm(fetch_impl):
    """AsyncStealthySession context-manager mock whose inner .fetch uses fetch_impl."""
    cm = AsyncMock()
    inner = cm.__aenter__.return_value
    inner.fetch = AsyncMock(side_effect=fetch_impl)
    return cm


# ── no-hook path (AC-02) ──────────────────────────────────────────────────────


class TestNoHookPathUnchanged:
    """Without retry_hook/max_retries the legacy behavior must survive untouched."""

    @patch.object(WebFetcher, "fetch")
    def test_normal_fast_scrape_without_hook_delivers_every_url(self, mock_fetch):
        mock_fetch.return_value = "<html>Clean</html>"
        fetcher = WebFetcher()
        results = []
        fetcher.scrape([A, B, C], callback=lambda url, html: results.append(url))
        assert sorted(results) == [A, B, C]

    @patch.object(WebFetcher, "_dispatch_scrape")
    def test_edge_hook_without_max_retries_is_inert(self, mock_dispatch):
        fetcher = WebFetcher()
        hook = MagicMock(return_value=None)
        fetcher.scrape([A], callback=MagicMock(), retry_hook=hook, max_retries=0)
        mock_dispatch.assert_called_once()
        hook.assert_not_called()

    @patch.object(WebFetcher, "fetch")
    def test_error_invalid_mode_still_raises_when_hook_is_configured(self, mock_fetch):
        fetcher = WebFetcher()
        hook = MagicMock(return_value="reason")
        with pytest.raises(ValueError, match="Unsupported scrape mode"):
            fetcher.scrape([A], callback=MagicMock(), mode="invalid", retry_hook=hook, max_retries=2)
        mock_fetch.assert_not_called()
        hook.assert_not_called()

    def test_error_max_retries_without_hook_raises_value_error(self):
        fetcher = WebFetcher()
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            fetcher.scrape([A], callback=MagicMock(), max_retries=-1)


# ── hook verdicts ─────────────────────────────────────────────────────────────


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
            caplog.at_level(logging.WARNING, logger="scrape_kit.fetcher.batch"),
            patch.object(fetcher, "fetch", fetch_mock),
            # AC-07: the permanently failing URL raises on the final pass only.
            pytest.raises(FetcherError, match="Fast scrape had 1 failures"),
        ):
            fetcher.scrape([A, GHOST], callback=lambda url, html: None, retry_hook=hook, max_retries=1)
        urls = [c.args[0] for c in fetch_mock.call_args_list]
        # AC-04: the deferred URL is re-fetched on the second pass.
        assert urls.count(A) == 2
        # AC-07: the failing URL is retried on both passes (twice per fast pass).
        assert urls.count(GHOST) == 4
        # AC-08: the deferred URL reaches the hook on both passes.
        assert [reason for _, reason in hook.calls] == ["reason-a", "reason-a"]
        assert any(record.levelno == logging.WARNING for record in caplog.records)

    @patch.object(WebFetcher, "fetch")
    def test_error_hook_exception_propagates(self, mock_fetch):
        mock_fetch.return_value = "<html>Clean</html>"
        fetcher = WebFetcher()

        def bad_hook(url: str, html: str) -> str | None:
            raise RuntimeError("hook bug")

        with pytest.raises(RuntimeError, match="hook bug"):
            fetcher.scrape([A], callback=MagicMock(), retry_hook=bad_hook, max_retries=3)
        assert mock_fetch.call_count == 1  # hook failure is not a fetch failure


# ── exhaustion policy ─────────────────────────────────────────────────────────


class TestExhaustionPolicy:
    """Final attempt delivers the rejected page once and warns."""

    def test_normal_persistent_hook_rejection_delivers_with_warning(self, caplog):
        fetcher = WebFetcher()
        hook = _reason_hook(**{A: "thin-content"})
        fetch_mock = MagicMock(return_value="<html>reject</html>")
        with (
            patch.object(fetcher, "fetch", fetch_mock),
            caplog.at_level(logging.WARNING, logger="scrape_kit.fetcher.batch"),
        ):
            delivered = []
            fetcher.scrape(
                [A],
                callback=lambda url, html: delivered.append(url),
                retry_hook=hook,
                max_retries=2,
            )
        assert delivered == [A]  # skipped on the rejecting passes, delivered once at the end
        assert [reason for _, reason in hook.calls] == ["thin-content", "thin-content", "thin-content"]
        fetched = [c.args[0] for c in fetch_mock.call_args_list]
        assert fetched == [A, A, A]  # one fetch per pass; nothing dropped
        assert any("thin-content" in record.message and A in record.message for record in caplog.records)
        assert any(record.levelno == logging.WARNING for record in caplog.records)


# ── stealth parity ────────────────────────────────────────────────────────────


class TestStealthParity:
    """The same semantics hold when mode=stealth re-runs the stealth loop."""

    @patch("asyncio.sleep", new_callable=AsyncMock)
    @patch("scrape_kit.fetcher.batch.AsyncStealthySession")
    def test_normal_stealth_hook_rejection_reenters_stealth_loop(self, mock_session, mock_sleep):
        attempts: dict[str, int] = {}
        seen: list[tuple[str, str]] = []

        def bad(url: str, html: str) -> str | None:
            """Reject fresh pages, accept the page carrying the settled marker."""
            reason = None if "accepted" in html else "reason-a"
            seen.append((url, reason or ""))
            return reason

        async def fetch_impl(url, **_kwargs):
            if url == A and attempts.get(A, 0) >= 2:
                return _stealth_page(url, attempts, html="<html>accepted</html>")
            return _stealth_page(url, attempts)

        mock_session.return_value = _stealth_cm(fetch_impl)
        fetcher = WebFetcher()
        delivered = []
        fetcher.scrape(
            [A],
            callback=lambda url, html: delivered.append(url),
            mode="stealth",
            retry_hook=bad,
            max_retries=2,
            max_concurrency=1,
        )
        assert delivered == [A]  # accepted on pass 3, delivered exactly once
        assert [reason for _, reason in seen] == ["reason-a", "reason-a", ""]
        assert mock_session.call_count == 3
        for call in mock_session.call_args_list:
            assert call.kwargs["max_pages"] == 1
        mock_sleep.assert_not_awaited()

    @patch("asyncio.sleep", new_callable=AsyncMock)
    @patch("scrape_kit.fetcher.batch.AsyncStealthySession")
    def test_error_stealth_final_attempt_raises(self, mock_session, mock_sleep):
        async def fetch_impl(url, **_kwargs):
            raise RuntimeError("page crashed")

        mock_session.return_value = _stealth_cm(fetch_impl)
        fetcher = WebFetcher()
        with pytest.raises(FetcherError, match="Stealth scrape had 1 failures"):
            fetcher.scrape(
                [A],
                callback=lambda url, html: None,
                mode="stealth",
                retry_hook=lambda url, html: "reason-a",
                max_retries=2,
                max_concurrency=1,
            )
        assert mock_session.call_count == 3


# ── guards and concurrency ────────────────────────────────────────────────────


class TestGuardsAndConcurrency:
    """Empty input, argument validation, and threaded FAST batches with mixed verdicts."""

    @patch.object(WebFetcher, "fetch")
    def test_normal_fast_concurrent_mixed_verdicts_are_exact(self, mock_fetch):
        fetcher = WebFetcher()
        hook = _reason_hook(**{A: "reason-a", C: "reason-c"})

        def _fetch(url: str, **_kwargs: str) -> str:
            return f"<html>{url}</html>"

        fetch_mock = MagicMock(side_effect=_fetch)
        with patch.object(fetcher, "fetch", fetch_mock):
            delivered = []
            fetcher.scrape(
                [A, B, C, GHOST],
                callback=lambda url, html: delivered.append(url),
                retry_hook=hook,
                max_retries=1,
                max_concurrency=4,
            )
        # AC-10: every URL is delivered exactly once — accepted URLs on pass 1,
        # deferred URLs on the final pass with a warning.
        assert len(delivered) == 4
        assert delivered.count(B) == 1
        assert delivered.count(GHOST) == 1
        assert delivered.count(A) == 1
        assert delivered.count(C) == 1
        fetched_order = [c.args[0] for c in fetch_mock.call_args_list]
        assert sorted(fetched_order[:4]) == sorted([A, B, C, GHOST])  # pass 1 fetches everything
        assert sorted(fetched_order[4:]) == sorted([A, C])  # pass 2 fetches only deferred URLs
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
        with patch.object(fetcher, "fetch", failing), pytest.raises(FetcherError, match="Fast scrape had 1 failures"):
            fetcher.scrape(
                [A, GHOST],
                callback=lambda url, html: None,
                retry_hook=hook,
                max_retries=1,
            )
        fetched = [c.args[0] for c in failing.call_args_list]
        # Each fast pass tries a URL twice internally; two passes run in total.
        assert fetched == [A, GHOST, GHOST, A, GHOST, GHOST]
        assert [reason for _, reason in hook.calls] == ["reason-a", "reason-a"]


# ── proxy forwarding ──────────────────────────────────────────────────────────


class TestProxyForwarding:
    """The module-level proxies accept and forward the new keyword arguments."""

    def test_normal_module_scrape_proxy_forwards_kwargs(self):
        fetcher_module.reset_shared()
        with patch.object(WebFetcher, "scrape", autospec=True) as mock_scrape:
            fetcher_module.scrape(
                [A],
                callback=MagicMock(),
                retry_hook=lambda url, html: None,
                max_retries=3,
            )
            mock_scrape.assert_called_once()
            assert mock_scrape.call_args.kwargs["retry_hook"] is not None
            assert mock_scrape.call_args.kwargs["max_retries"] == 3

    def test_normal_package_scrape_proxy_forwards_kwargs(self):
        fetcher_module.reset_shared()
        with patch.object(WebFetcher, "scrape", autospec=True) as mock_scrape:
            scrape_kit.scrape(
                [A],
                callback=MagicMock(),
                retry_hook=lambda url, html: None,
                max_retries=2,
            )
            assert mock_scrape.call_args.kwargs["max_retries"] == 2

    def test_normal_retry_hook_alias_is_the_documented_type(self):
        assert callable(RetryHook)
        assert scrape_kit.RetryHook is fetcher_module.RetryHook


# ── AC-09: existing retry layers still apply ──────────────────────────────────


class TestExistingLayersStillApply:
    """Hook retries stay additive: the fast second chance and escalation are untouched."""

    @patch.object(WebFetcher, "fetch")
    def test_normal_fast_second_chance_still_runs_under_hook(self, mock_fetch):
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
        assert mock_fetch.call_count == 2  # blocked first attempt, stealthy second
        assert mock_fetch.call_args_list[0].kwargs["stealthy_headers"] is False
        assert mock_fetch.call_args_list[1].kwargs["stealthy_headers"] is True
        assert results == [A]
        assert [c.args[0] for c in hook.call_args_list] == [A]  # hook sees only the clean page

    @patch.object(WebFetcher, "browser")
    @patch.object(WebFetcher, "_fetch_attempt")
    def test_normal_browser_escalation_still_runs_under_hook(self, mock_attempt, mock_browser):
        fetcher = WebFetcher(retry_indicators=["Just a moment"])
        # Attempt 1 signals a retry, attempt 2 returns clean HTML.
        mock_attempt.side_effect = [None, "<html>Clean</html>"]

        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock()
        session.fetch.return_value = "<html>escalated</html>"
        mock_browser.return_value = session

        hook = MagicMock(return_value=None)
        results = []
        fetcher.scrape(
            [A],
            callback=lambda url, html: results.append(url),
            retry_hook=hook,
            max_retries=1,
        )
        assert mock_attempt.call_count == 2  # the inner fetch retry loop still runs
        assert results == [A]
        assert [c.args[0] for c in hook.call_args_list] == [A]
        mock_browser.assert_not_called()

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep", return_value=None)
    @patch.object(WebFetcher, "_retry_wait", return_value=0.0)
    @patch.object(WebFetcher, "browser")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher.get")
    def test_normal_escalation_serves_the_hook_the_escalated_html(self, mock_get, mock_browser, mock_wait, mock_sleep):
        fetcher = WebFetcher(retry_indicators=["Just a moment"])
        # The transport serves the retry indicator for every inner attempt.
        mock_get.return_value = make_page("<html>Just a moment</html>", status=200)

        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock()
        session.fetch.return_value = "<html>escalated-via-browser</html>"
        mock_browser.return_value = session

        hook = MagicMock(return_value=None)
        results = []
        fetcher.scrape(
            [A],
            callback=lambda url, html: results.append(url),
            retry_hook=hook,
            max_retries=1,
        )
        assert mock_browser.call_count == 1  # the escalation path is untouched
        assert mock_get.call_count == 3  # three inner attempts before escalation
        assert results == [A]
        assert [c.args[0] for c in hook.call_args_list] == [A]


# ── CR regressions: user-code FetcherError, duplicates, guard parity ──────────


class TestUserCodeFailureContract:
    """Hook/callback FetcherError must escape aggregation and the retry loop."""

    @patch.object(WebFetcher, "fetch")
    def test_error_hook_raising_fetcher_error_propagates_immediately(self, mock_fetch):
        mock_fetch.return_value = "<html>Clean</html>"
        fetcher = WebFetcher()

        def bad_hook(url: str, html: str) -> str | None:
            raise FetcherError("hook-domain-failure")

        with pytest.raises(Exception) as exc_info:
            fetcher.scrape([A], callback=MagicMock(), retry_hook=bad_hook, max_retries=3)
        # Not collected as a fetch failure and not retried.
        assert not isinstance(exc_info.value, FetcherError) or "Fast scrape had" not in str(exc_info.value)
        assert mock_fetch.call_count == 1
        assert isinstance(exc_info.value.__cause__, FetcherError)

    @patch.object(WebFetcher, "fetch")
    def test_error_callback_raising_fetcher_error_propagates_immediately(self, mock_fetch):
        mock_fetch.return_value = "<html>Clean</html>"
        fetcher = WebFetcher()
        calls: list[str] = []

        def bad_callback(url: str, html: str) -> None:
            calls.append(url)
            raise FetcherError("callback-domain-failure")

        with pytest.raises(Exception) as exc_info:
            fetcher.scrape([A], callback=bad_callback, retry_hook=lambda url, html: None, max_retries=2)
        assert not isinstance(exc_info.value, FetcherError) or "Fast scrape had" not in str(exc_info.value)
        assert calls == [A]  # the callback never re-ran on a retry pass
        assert mock_fetch.call_count == 1
        assert isinstance(exc_info.value.__cause__, FetcherError)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    @patch("scrape_kit.fetcher.batch.AsyncStealthySession")
    def test_error_stealth_hook_raising_fetcher_error_propagates(self, mock_session, mock_sleep):
        def bad_hook(url: str, html: str) -> str | None:
            raise FetcherError("hook-domain-failure")

        attempts: dict[str, int] = {}
        mock_session.return_value = _stealth_cm(lambda url, **_kw: _stealth_page(url, attempts))
        fetcher = WebFetcher()
        with pytest.raises(Exception) as exc_info:
            fetcher.scrape(
                [A],
                callback=MagicMock(),
                mode="stealth",
                retry_hook=bad_hook,
                max_retries=2,
                max_concurrency=1,
            )
        assert not isinstance(exc_info.value, FetcherError) or "Stealth scrape had" not in str(exc_info.value)
        assert mock_session.call_count == 1  # never re-entered the stealth loop
        assert isinstance(exc_info.value.__cause__, FetcherError)


class TestDuplicateUrlBookkeeping:
    """Per-entry retry accounting: a failed duplicate is retried, not dropped."""

    def test_normal_transient_failure_on_duplicate_is_retried_not_dropped(self):
        fetcher = WebFetcher()
        seen: list[str] = []

        def _fetch(url: str, **_kwargs: str) -> str:
            seen.append(url)
            if url == A and seen.count(A) <= 2:
                raise RuntimeError("transient")
            return f"<html>ok-{url}</html>"

        fetch_mock = MagicMock(side_effect=_fetch)
        with patch.object(fetcher, "fetch", fetch_mock):
            delivered: list[str] = []
            fetcher.scrape(
                [A, A],
                callback=lambda url, html: delivered.append(url),
                retry_hook=lambda url, html: None,
                max_retries=1,
            )
        # Both entries are delivered: pass 2 retries the entry that failed, not the whole set.
        assert delivered == [A, A]

    @patch.object(WebFetcher, "fetch")
    def test_error_max_concurrency_zero_with_hook_raises_before_fetch(self, mock_fetch):
        fetcher = WebFetcher()
        hook = MagicMock(return_value=None)
        with pytest.raises(ValueError, match="max_concurrency must be >= 1"):
            fetcher.scrape([A], callback=MagicMock(), retry_hook=hook, max_retries=1, max_concurrency=0)
        mock_fetch.assert_not_called()
        hook.assert_not_called()
