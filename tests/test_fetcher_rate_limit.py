"""429 rate-limit interception: status survival, Retry-After waits, batch cooldown."""

import asyncio
import contextlib
import email.utils
import threading
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import ESCALATE_TIMEOUT_MS, make_page

from scrape_kit.errors import FetcherError
from scrape_kit.fetcher.batch import ScrapeMode, _BatchCooldown
from scrape_kit.fetcher.session import InteractiveSession
from scrape_kit.fetcher.web_fetcher import WebFetcher

pytestmark = pytest.mark.p0

URL = "http://test.com"
OTHER = "http://other.com"


# ── AC-01: navigation status survives the browser path ────────────────────────


class TestSessionStatusCapture:
    """InteractiveSession caches the goto response and exposes its status."""

    def _session(self, response, html="<html>page</html>"):
        """Build an entered InteractiveSession whose page.goto returns response."""
        inner = MagicMock()
        page = MagicMock()
        page.goto.return_value = response
        page.content.return_value = getattr(response, "html_content", html)
        page.evaluate.return_value = None
        inner.context.new_page.return_value = page
        session = InteractiveSession(inner)
        session.__enter__()
        # The settle loop needs one evaluate resolution to finish fetching.
        page.evaluate.return_value = None
        with contextlib.suppress(Exception):
            session.fetch(URL)
        return session

    def test_normal_status_two_hundred_cached(self):
        """AC-01: a 2xx navigation exposes status 200."""
        session = self._session(make_page("<html>ok</html>", status=200))
        assert session._last_response_status() == 200

    def test_normal_status_429_cached(self):
        """AC-01: a 429 navigation exposes the status though HTML is returned."""
        session = self._session(make_page("<html>slow down</html>", status=429))
        assert session.fetch(URL) == "<html>slow down</html>"
        assert session._last_response_status() == 429

    def test_normal_status_prefers_status_over_status_code(self):
        """AC-01: the status attribute wins over status_code."""
        response = MagicMock()
        response.status = 503
        response.status_code = 200
        session = self._session(response)
        assert session._last_response_status() == 503

    def test_edge_response_without_status_returns_none(self):
        """AC-01: a response with neither status attribute reports None."""
        session = self._session(MagicMock(spec=[]))
        assert session._last_response_status() is None

    def test_edge_failed_navigation_reports_no_stale_status(self):
        """AC-01: a raising goto clears the cache, never reporting a stale status."""
        inner = MagicMock()
        page = MagicMock()
        page.goto.side_effect = Exception("navigation failed")
        inner.context.new_page.return_value = page
        session = InteractiveSession(inner)
        session.__enter__()

        page.goto.side_effect = None
        page.goto.return_value = make_page(status=429)
        session.fetch(URL)
        assert session._last_response_status() == 429

        page.goto.side_effect = Exception("navigation failed")
        with pytest.raises(Exception, match="navigation failed"):
            session.fetch(URL)
        assert session._last_response_status() is None

    def test_normal_retry_after_seconds_read_from_headers(self):
        """AC-04: the session exposes the seconds form of Retry-After."""
        response = make_page(status=429)
        response.headers = {"Retry-After": "30"}
        session = self._session(response)
        assert session._last_retry_after() == 30.0

    def test_edge_retry_after_invalid_returns_none(self):
        """AC-06: an unparsable header exposes None."""
        response = make_page(status=429)
        response.headers = {"Retry-After": "soon"}
        session = self._session(response)
        assert session._last_retry_after() is None


# ── AC-02/AC-03: escalation gate ──────────────────────────────────────────────


class TestEscalationStatusGate:
    """_escalate_to_browser never delivers a 429/503 body as success."""

    def _browser_session(self, html, status):
        """Mock browser context manager exposing a cached navigation status."""
        browser_session = MagicMock()
        browser_session.fetch.return_value = html
        browser_session._last_response_status.return_value = status
        browser_session._last_retry_after.return_value = None
        browser_session.__enter__ = MagicMock(return_value=browser_session)
        browser_session.__exit__ = MagicMock(return_value=False)
        return browser_session

    def test_normal_escalation_2xx_returns_html(self):
        """AC-03: the 2xx path is unchanged."""
        fetcher = WebFetcher()
        browser_session = self._browser_session("<html>bypassed</html>", 200)
        with patch.object(fetcher, "browser", return_value=browser_session):
            result = fetcher._escalate_to_browser(URL, "just a moment")
        assert result == "<html>bypassed</html>"
        browser_session.fetch.assert_called_once_with(URL, timeout=ESCALATE_TIMEOUT_MS)

    def test_error_escalation_429_raises_rate_limited(self):
        """AC-02: a browser 429 raises FetcherError instead of returning the body."""
        fetcher = WebFetcher()
        browser_session = self._browser_session("<html>rate limited</html>", 429)
        with (
            patch.object(fetcher, "browser", return_value=browser_session),
            pytest.raises(FetcherError, match="rate limited") as excinfo,
        ):
            fetcher._escalate_to_browser(URL, "just a moment")
        assert excinfo.value.url == URL
        assert "429" in str(excinfo.value)

    def test_error_escalation_503_raises(self):
        """AC-02: a browser 503 raises FetcherError with url set."""
        fetcher = WebFetcher()
        browser_session = self._browser_session("<html>unavailable</html>", 503)
        with (
            patch.object(fetcher, "browser", return_value=browser_session),
            pytest.raises(FetcherError, match="503") as excinfo,
        ):
            fetcher._escalate_to_browser(URL, "just a moment")
        assert excinfo.value.url == URL

    def test_error_escalation_429_logs_retry_after_hint(self):
        """AC-02: the Retry-After hint is logged when the browser page exposes it."""
        fetcher = WebFetcher()
        browser_session = self._browser_session("<html>slow</html>", 429)
        browser_session._last_retry_after.return_value = 12.0
        with (
            patch.object(fetcher, "browser", return_value=browser_session),
            patch("scrape_kit.fetcher.web_fetcher.logger") as mock_log,
            pytest.raises(FetcherError),
        ):
            fetcher._escalate_to_browser(URL, "just a moment")
        logged = [str(call) for call in mock_log.warning.call_args_list]
        assert any("Retry-After" in text or "12" in text for text in logged), logged

    def test_error_escalation_429_not_wrapped_as_escalation_failure(self):
        """AC-02: the classified message is not re-wrapped by the generic handler."""
        fetcher = WebFetcher()
        browser_session = self._browser_session("<html>slow</html>", 429)
        with (
            patch.object(fetcher, "browser", return_value=browser_session),
            pytest.raises(FetcherError) as excinfo,
        ):
            fetcher._escalate_to_browser(URL, "just a moment")
        assert "Escalation failed" not in str(excinfo.value)


# ── AC-04/05/06/07/08: retry waits and exhaustion messages ───────────────────


def _page_with_retry_after(status, header):
    """Build a fake scrapling page with a status and a Retry-After header."""
    page = MagicMock()
    page.status = status
    page.status_code = status
    page.html_content = "<html>body</html>"
    page.headers = {} if header is None else {"Retry-After": header}
    return page


class TestRetryAfterWait:
    """The 429 wait honors Retry-After and falls back to the jittered wait."""

    @patch("scrape_kit.fetcher.web_fetcher.random.uniform", return_value=1.0)
    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_seconds_header_drives_wait(self, mock_fetcher, mock_sleep, _jitter):
        """AC-04: a seconds Retry-After is slept instead of the jittered value."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(429, "30"), make_page("<html>ok</html>")]
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            result = fetcher.fetch(URL, retries=3, backoff=5.0)
        assert result == "<html>ok</html>"
        assert mock_fetcher.get.call_count == 2
        mock_sleep.assert_called_once_with(30.0)

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_http_date_header_drives_wait(self, mock_fetcher, mock_sleep):
        """AC-05: an HTTP-date Retry-After is parsed into a bounded delay."""
        future = email.utils.formatdate(time.time() + 45, usegmt=True)
        mock_fetcher.get.side_effect = [_page_with_retry_after(429, future), make_page("<html>ok</html>")]
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            result = fetcher.fetch(URL, retries=3, backoff=5.0)
        assert result == "<html>ok</html>"
        assert mock_sleep.call_count == 1
        assert 0 < mock_sleep.call_args.args[0] <= 60

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_edge_past_http_date_falls_back_to_jitter(self, mock_fetcher, mock_sleep):
        """AC-05: a past date falls back to the jittered wait."""
        past = email.utils.formatdate(time.time() - 600, usegmt=True)
        mock_fetcher.get.side_effect = [_page_with_retry_after(429, past), make_page("<html>ok</html>")]
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            result = fetcher.fetch(URL, retries=3, backoff=5.0)
        assert result == "<html>ok</html>"
        assert mock_sleep.call_count == 1
        assert 0 < mock_sleep.call_args.args[0] <= 5.0 * 1 * 1.5

    @patch("scrape_kit.fetcher.web_fetcher.random.uniform", return_value=1.0)
    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_missing_header_uses_jitter(self, mock_fetcher, mock_sleep, _jitter):
        """AC-06: no header means the existing jittered wait applies."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(429, None), make_page("<html>ok</html>")]
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            result = fetcher.fetch(URL, retries=3, backoff=5.0)
        assert result == "<html>ok</html>"
        mock_sleep.assert_called_once_with(5.0)

    @patch("scrape_kit.fetcher.web_fetcher.random.uniform", return_value=1.0)
    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_edge_invalid_header_uses_jitter(self, mock_fetcher, mock_sleep, _jitter):
        """AC-06: an unparsable header falls back to the jittered wait."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(429, "soon"), make_page("<html>ok</html>")]
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        mock_sleep.assert_called_once_with(5.0)

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_edge_absurd_seconds_header_is_capped(self, mock_fetcher, mock_sleep):
        """AC-04/AC-10: an absurd seconds value is capped, never slept verbatim."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(429, "999999"), make_page("<html>ok</html>")]
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        assert mock_sleep.call_args.args[0] <= 300

    @patch("scrape_kit.fetcher.web_fetcher.random.uniform", return_value=1.0)
    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_503_keeps_jitter_and_ignores_header(self, mock_fetcher, mock_sleep, _jitter):
        """AC-03/AC-08: 503 ignores Retry-After and keeps the jittered wait."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(503, "30"), make_page("<html>ok</html>")]
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        mock_sleep.assert_called_once_with(5.0)

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_error_429_exhaustion_labels_rate_limiting(self, mock_fetcher, mock_sleep):
        """AC-07: 429 exhaustion keeps legacy wording and adds the label."""
        mock_fetcher.get.return_value = _page_with_retry_after(429, "30")
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"), pytest.raises(FetcherError) as excinfo:
            fetcher.fetch(URL, retries=2, backoff=5.0)
        message = str(excinfo.value)
        assert "Blocked with status 429 on" in message
        assert "after 2 attempts" in message
        assert "rate limited" in message
        assert excinfo.value.url == URL

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_error_503_exhaustion_message_unchanged(self, mock_fetcher, mock_sleep):
        """AC-07/AC-08: non-429 exhaustion messages stay byte-identical."""
        mock_fetcher.get.return_value = _page_with_retry_after(503, "30")
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"), pytest.raises(FetcherError) as excinfo:
            fetcher.fetch(URL, retries=2, backoff=5.0)
        assert str(excinfo.value) == "Blocked with status 503 on http://test.com after 2 attempts"

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_attempt_budget_unchanged(self, mock_fetcher, mock_sleep):
        """AC-08: retries still mean the same number of attempts."""
        mock_fetcher.get.return_value = _page_with_retry_after(429, "30")
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"), pytest.raises(FetcherError):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        assert mock_fetcher.get.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_429_records_rate_limit_signal(self, mock_fetcher, mock_sleep):
        """AC-09: the fast loop records the 429 for the batch cooldown layer."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(429, "30"), make_page("<html>ok</html>")]
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        signal = fetcher._take_rate_limit_signal()
        assert signal is not None
        assert signal.url == URL
        assert 0 < signal.retry_after <= 300
        assert fetcher._take_rate_limit_signal() is None


# ── AC-09/10/12: batch cooldown primitive ─────────────────────────────────────


class TestBatchCooldownUnit:
    """The shared primitive bounds, caps, and never shrinks its deadline."""

    def test_normal_register_extends_deadline(self):
        """AC-09/AC-10: a later registration never shortens the deadline."""
        cooldown = _BatchCooldown(default_wait=10.0, max_wait=300.0)
        now = time.monotonic()
        cooldown.register(now, retry_after=20.0)
        cooldown.register(now, retry_after=5.0)
        assert cooldown.remaining() > 15

    def test_edge_max_wait_caps_registration(self):
        """AC-10: registration is capped by the configured maximum."""
        cooldown = _BatchCooldown(default_wait=10.0, max_wait=60.0)
        cooldown.register(time.monotonic(), retry_after=10_000.0)
        assert cooldown.remaining() <= 60.0

    def test_normal_no_registration_never_waits(self):
        """AC-13: no signal means no wait."""
        assert _BatchCooldown(default_wait=10.0, max_wait=300.0).remaining() == 0.0

    def test_normal_remaining_shrinks_with_time(self):
        """AC-09: the remaining wait is measured against a monotonic clock."""
        cooldown = _BatchCooldown(default_wait=10.0, max_wait=300.0)
        now = time.monotonic()
        cooldown.register(now, retry_after=30.0)
        first = cooldown.remaining()
        with patch("scrape_kit.fetcher.batch.time.monotonic", return_value=now + 10):
            assert cooldown.remaining() <= 20
            later = cooldown.remaining()
        assert first > later

    def test_normal_register_from_signal_uses_header(self):
        """AC-04/AC-09: a header-derived signal sets the deadline from Retry-After."""
        cooldown = _BatchCooldown(default_wait=10.0, max_wait=300.0)
        cooldown.register(time.monotonic() + 60, retry_after=25.0)
        assert cooldown.remaining() > 20

    def test_normal_register_from_signal_falls_back_to_default(self):
        """AC-10: a signal without a header uses the bounded default wait."""
        cooldown = _BatchCooldown(default_wait=10.0, max_wait=300.0)
        cooldown.register(time.monotonic(), retry_after=None)
        assert 9.0 < cooldown.remaining() <= 10.0


class TestFastBatchCooldown:
    """FAST workers wait out the shared deadline before the next request."""

    def test_normal_429_pauses_other_workers(self):
        """AC-09: one 429 makes the sibling worker wait before its own request."""
        fetcher = WebFetcher()
        order: list[str] = []
        lock = threading.Lock()

        def fake_fetch(url, **_kwargs):
            if url == URL:
                with lock:
                    order.append("fetch-limited")
                raise FetcherError(f"Blocked with status 429 on {url} after 3 attempts (rate limited)", url=url)
            while "fetch-limited" not in order:
                time.sleep(0.001)
            with lock:
                order.append("fetch-other")
            return "<html>clean</html>"

        def fake_wait(self):
            if self.remaining() <= 0:
                return
            with lock:
                order.append("cooldown-wait")
            time.sleep(0.05)

        signals = [MagicMock(url=URL, retry_after=20.0)] + [None] * 8
        with (
            patch.object(WebFetcher, "fetch", side_effect=fake_fetch),
            patch.object(WebFetcher, "_take_rate_limit_signal", side_effect=signals),
            patch("scrape_kit.fetcher.batch._BatchCooldown.wait_if_needed", fake_wait),
            pytest.raises(FetcherError),
        ):
            fetcher.scrape([URL, OTHER], lambda url, html: None, max_concurrency=2)
        assert order.count("cooldown-wait") >= 1
        assert order.index("fetch-limited") < order.index("cooldown-wait")

    def test_error_429_url_reported_in_summary(self):
        """AC-12: the cooldown never swallows the failing URL."""
        fetcher = WebFetcher()

        def fake_fetch(url, **_kwargs):
            raise FetcherError(f"Blocked with status 429 on {url} after 3 attempts (rate limited)", url=url)

        with (
            patch.object(WebFetcher, "fetch", side_effect=fake_fetch),
            patch.object(WebFetcher, "_take_rate_limit_signal", return_value=None),
            pytest.raises(FetcherError, match="Fast scrape had 2 failures"),
        ):
            fetcher.scrape([URL, "http://two.com"], lambda url, html: None, max_concurrency=2)

    def test_normal_no_429_registers_nothing(self):
        """AC-13: a clean batch creates a cooldown that never waits."""
        fetcher = WebFetcher()
        delivered: list[str] = []
        created: list[_BatchCooldown] = []
        real_init = _BatchCooldown.__init__

        def recording_init(self, *args, **kwargs):
            real_init(self, *args, **kwargs)
            created.append(self)

        with (
            patch.object(WebFetcher, "fetch", return_value="<html>clean</html>"),
            patch.object(_BatchCooldown, "__init__", recording_init),
        ):
            fetcher.scrape([URL, "http://two.com"], lambda url, html: delivered.append(url), max_concurrency=2)
        assert sorted(delivered) == sorted([URL, "http://two.com"])
        assert len(created) == 1
        assert created[0].remaining() == 0.0


class TestStealthBatchCooldown:
    """STEALTH workers await the same shared deadline before the next request."""

    def test_normal_stealth_429_pauses_sibling_worker(self):
        """AC-09: the async path honors the same per-call cooldown."""
        fetcher = WebFetcher()
        order: list[str] = []
        lock = threading.Lock()

        class FakeSession:
            """Minimal async session exposing per-URL pages."""

            def __init__(self, **_kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def fetch(self, url, **_kwargs):
                with lock:
                    order.append(f"fetch:{url}")
                if url == URL:
                    return _page_with_retry_after(429, "30")
                return make_page("<html>clean</html>")

        real_wait = _BatchCooldown.wait_if_needed_async

        async def fake_wait(self):
            if self.remaining() <= 0:
                return
            with lock:
                order.append("cooldown-wait")
            await real_wait(self)

        with (
            patch("scrape_kit.fetcher.batch.AsyncStealthySession", FakeSession),
            patch.object(_BatchCooldown, "wait_if_needed_async", fake_wait),
            patch("scrape_kit.fetcher.batch.asyncio.sleep", AsyncMock(return_value=None)),
            pytest.raises(FetcherError),
        ):
            fetcher.scrape(
                [URL, OTHER],
                lambda url, html: None,
                mode=ScrapeMode.STEALTH,
                max_concurrency=2,
            )
        assert "cooldown-wait" in order
        assert order.index(f"fetch:{URL}") < order.index("cooldown-wait")

    def test_error_stealth_429_url_reported(self):
        """AC-12: the async summary still reports the rate-limited URL."""
        fetcher = WebFetcher()

        class FakeSession:
            """Async session that always returns a 429 page."""

            def __init__(self, **_kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def fetch(self, url, **_kwargs):
                return _page_with_retry_after(429, "30")

        with (
            patch("scrape_kit.fetcher.batch.AsyncStealthySession", FakeSession),
            patch.object(_BatchCooldown, "wait_if_needed_async", AsyncMock(return_value=None)),
            patch("scrape_kit.fetcher.batch.asyncio.sleep", AsyncMock(return_value=None)),
            pytest.raises(FetcherError, match="Stealth scrape had 1 failures"),
        ):
            fetcher.scrape([URL], lambda url, html: None, mode=ScrapeMode.STEALTH, max_concurrency=1)


class TestPerBatchIsolation:
    """Independent scrape() calls never throttle each other."""

    def test_normal_separate_cooldowns(self):
        """AC-11: each scrape() call owns its cooldown primitive."""
        fetcher = WebFetcher()
        seen: list[_BatchCooldown] = []
        real_init = _BatchCooldown.__init__

        def recording_init(self, *args, **kwargs):
            real_init(self, *args, **kwargs)
            seen.append(self)

        with (
            patch.object(WebFetcher, "fetch", return_value="<html>clean</html>"),
            patch.object(_BatchCooldown, "__init__", recording_init),
        ):
            fetcher.scrape([URL], lambda url, html: None)
            fetcher.scrape([URL], lambda url, html: None)
        assert len(seen) == 2
        assert seen[0] is not seen[1]


class TestGuardsUnchanged:
    """AC-13: guards and callback delivery are unchanged."""

    def test_edge_empty_urls_no_fetch(self):
        """AC-13: empty URL lists return before any fetch."""
        fetcher = WebFetcher()
        delivered: list[str] = []
        with patch.object(WebFetcher, "fetch") as mock_fetch:
            fetcher.scrape([], lambda url, html: delivered.append(url))
        mock_fetch.assert_not_called()
        assert delivered == []

    def test_error_bad_mode_raises_before_fetch(self):
        """AC-13: an unsupported mode still raises ValueError first."""
        fetcher = WebFetcher()
        with (
            patch.object(WebFetcher, "fetch") as mock_fetch,
            pytest.raises(ValueError, match="Unsupported scrape mode"),
        ):
            fetcher.scrape([URL], lambda url, html: None, mode="nope")
        mock_fetch.assert_not_called()

    def test_normal_clean_batch_delivers_every_url(self):
        """AC-12: every clean URL is delivered exactly once."""
        fetcher = WebFetcher()
        delivered: list[str] = []
        urls = ["http://a.com", "http://b.com", "http://c.com"]
        with patch.object(WebFetcher, "fetch", return_value="<html>clean</html>"):
            fetcher.scrape(urls, lambda url, html: delivered.append(url), max_concurrency=3)
        assert sorted(delivered) == sorted(urls)


class TestAsyncWaitAdapter:
    """The async cooldown adapter awaits the same deadline."""

    def test_normal_async_adapter_sleeps_remaining(self):
        """AC-09: wait_if_needed_async sleeps the remaining deadline."""
        cooldown = _BatchCooldown(default_wait=10.0, max_wait=300.0)
        cooldown.register(time.monotonic(), retry_after=5.0)
        slept: list[float] = []

        async def fake_sleep(seconds):
            slept.append(seconds)

        async def run():
            await cooldown.wait_if_needed_async()

        with patch("scrape_kit.fetcher.batch.asyncio.sleep", fake_sleep):
            asyncio.run(run())
        assert slept
        assert 0 < slept[0] <= 5

    def test_normal_async_adapter_no_sleep_when_clear(self):
        """AC-13: no deadline means the async adapter returns immediately."""
        cooldown = _BatchCooldown(default_wait=10.0, max_wait=300.0)
        slept: list[float] = []

        async def fake_sleep(seconds):
            slept.append(seconds)

        async def run():
            await cooldown.wait_if_needed_async()

        with patch("scrape_kit.fetcher.batch.asyncio.sleep", fake_sleep):
            asyncio.run(run())
        assert slept == []


# ── CR: stale-signal and wording regressions ──────────────────────────────────


class TestStaleSignalIsolation:
    """A 429 observed outside a batch must never throttle that batch."""

    def test_edge_stale_signal_from_single_fetch_is_ignored(self):
        """AC-13: a single-URL 429 cannot throttle the next scrape() call."""
        fetcher = WebFetcher()
        # Simulate the slot a single-URL fetch() leaves behind (AC-09 path).
        fetcher._note_rate_limit("http://limited.example", 30.0)
        assert fetcher._take_rate_limit_signal() is not None
        fetcher._note_rate_limit("http://limited.example", 30.0)
        delivered: list[str] = []

        def fake_fetch(url, **_kwargs):
            return "<html>clean</html>"

        waits: list[float] = []

        def fake_wait(self):
            waits.append(self.remaining())

        with (
            patch.object(WebFetcher, "fetch", side_effect=fake_fetch),
            patch("scrape_kit.fetcher.batch._BatchCooldown.wait_if_needed", fake_wait),
        ):
            fetcher.scrape([URL, OTHER], lambda url, html: delivered.append(url), max_concurrency=2)
        assert sorted(delivered) == sorted([URL, OTHER])
        assert waits and all(w == 0.0 for w in waits)

    def test_normal_in_batch_signal_is_honored(self):
        """AC-09: a signal observed inside the batch still registers the cooldown."""
        fetcher = WebFetcher()
        observed: list[str] = []

        def fake_fetch(url, **_kwargs):
            if url == URL:
                fetcher._note_rate_limit(url, 15.0)
                raise FetcherError(f"Blocked with status 429 on {url} after 3 attempts (rate limited)", url=url)
            observed.append(url)
            return "<html>clean</html>"

        waits: list[float] = []

        def fake_wait(self):
            waits.append(self.remaining())

        with (
            patch.object(WebFetcher, "fetch", side_effect=fake_fetch),
            patch("scrape_kit.fetcher.batch._BatchCooldown.wait_if_needed", fake_wait),
            contextlib.suppress(FetcherError),
        ):
            fetcher.scrape([URL, OTHER], lambda url, html: None, max_concurrency=2)
        assert observed == [OTHER]
        assert waits and any(w > 0 for w in waits)


class TestEscalationWording:
    """The single-navigation browser path must not render 'after 1 attempts'."""

    def test_normal_escalation_429_message_omits_attempts(self):
        """AC-02/AC-07: the browser path message has no attempt clause."""
        fetcher = WebFetcher()

        def fake_fetch(url, **_kwargs):
            return "<html>rate limited</html>"

        session = MagicMock()
        session.__enter__.return_value = session
        session._last_response_status.return_value = 429
        session._last_retry_after.return_value = None

        with (
            patch.object(WebFetcher, "fetch", side_effect=fake_fetch),
            patch.object(WebFetcher, "browser", return_value=session),
            pytest.raises(FetcherError) as excinfo,
        ):
            fetcher._escalate_to_browser(URL, "retry indicator")
        message = str(excinfo.value)
        assert "(rate limited)" in message
        assert "after 1 attempts" not in message
        assert "after" not in message

    def test_normal_fast_path_attempt_clause_unchanged(self):
        """AC-07: the fast path still renders 'after N attempts' byte-identically."""
        fetcher = WebFetcher()
        message = fetcher._blocked_status_message(URL, 429, 3)
        assert message == f"Blocked with status 429 on {URL} after 3 attempts (rate limited)"
