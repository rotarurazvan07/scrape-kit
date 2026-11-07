"""429 rate-limit interception: status survival and Retry-After waits."""

import contextlib
import email.utils
import time
from unittest.mock import MagicMock, patch

import pytest
from conftest import ESCALATE_TIMEOUT_MS, make_page

from scrape_kit.errors import FetcherError
from scrape_kit.fetcher.session import InteractiveSession
from scrape_kit.fetcher.web_fetcher import WebFetcher

pytestmark = pytest.mark.p0

URL = "http://test.com"
OTHER = "http://other.com"


# ── Navigation status survives the browser path ────────────────────────


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
        """A 2xx navigation exposes status 200."""
        session = self._session(make_page("<html>ok</html>", status=200))
        assert session._last_response_status() == 200

    def test_normal_status_429_cached(self):
        """A 429 navigation exposes the status though HTML is returned."""
        session = self._session(make_page("<html>slow down</html>", status=429))
        assert session.fetch(URL) == "<html>slow down</html>"
        assert session._last_response_status() == 429

    def test_normal_status_prefers_status_over_status_code(self):
        """The status attribute wins over status_code."""
        response = MagicMock()
        response.status = 503
        response.status_code = 200
        session = self._session(response)
        assert session._last_response_status() == 503

    def test_edge_response_without_status_returns_none(self):
        """A response with neither status attribute reports None."""
        session = self._session(MagicMock(spec=[]))
        assert session._last_response_status() is None

    def test_edge_failed_navigation_reports_no_stale_status(self):
        """A raising goto clears the cache, never reporting a stale status."""
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
        """The session exposes the seconds form of Retry-After."""
        response = make_page(status=429)
        response.headers = {"Retry-After": "30"}
        session = self._session(response)
        assert session._last_retry_after() == 30.0

    def test_edge_retry_after_invalid_returns_none(self):
        """An unparsable header exposes None."""
        response = make_page(status=429)
        response.headers = {"Retry-After": "soon"}
        session = self._session(response)
        assert session._last_retry_after() is None


# ── Escalation gate ──────────────────────────────────────────────


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
        """The 2xx path is unchanged."""
        fetcher = WebFetcher()
        browser_session = self._browser_session("<html>bypassed</html>", 200)
        with patch.object(fetcher, "browser", return_value=browser_session):
            result = fetcher._escalate_to_browser(URL, "just a moment")
        assert result == "<html>bypassed</html>"
        browser_session.fetch.assert_called_once_with(URL, timeout=ESCALATE_TIMEOUT_MS)

    def test_error_escalation_429_raises_rate_limited(self):
        """A browser 429 raises FetcherError instead of returning the body."""
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
        """A browser 503 raises FetcherError with url set."""
        fetcher = WebFetcher()
        browser_session = self._browser_session("<html>unavailable</html>", 503)
        with (
            patch.object(fetcher, "browser", return_value=browser_session),
            pytest.raises(FetcherError, match="503") as excinfo,
        ):
            fetcher._escalate_to_browser(URL, "just a moment")
        assert excinfo.value.url == URL

    def test_error_escalation_429_logs_retry_after_hint(self):
        """The Retry-After hint is logged when the browser page exposes it."""
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
        """The classified message is not re-wrapped by the generic handler."""
        fetcher = WebFetcher()
        browser_session = self._browser_session("<html>slow</html>", 429)
        with (
            patch.object(fetcher, "browser", return_value=browser_session),
            pytest.raises(FetcherError) as excinfo,
        ):
            fetcher._escalate_to_browser(URL, "just a moment")
        assert "Escalation failed" not in str(excinfo.value)


# ── Retry waits and exhaustion messages ───────────────────


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
        """A seconds Retry-After is slept instead of the jittered value."""
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
        """An HTTP-date Retry-After is parsed into a bounded delay."""
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
        """A past date falls back to the jittered wait."""
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
        """No header means the existing jittered wait applies."""
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
        """An unparsable header falls back to the jittered wait."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(429, "soon"), make_page("<html>ok</html>")]
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        mock_sleep.assert_called_once_with(5.0)

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_edge_absurd_seconds_header_is_capped(self, mock_fetcher, mock_sleep):
        """An absurd seconds value is capped, never slept verbatim."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(429, "999999"), make_page("<html>ok</html>")]
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        assert mock_sleep.call_args.args[0] <= 300

    @patch("scrape_kit.fetcher.web_fetcher.random.uniform", return_value=1.0)
    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_503_keeps_jitter_and_ignores_header(self, mock_fetcher, mock_sleep, _jitter):
        """503 ignores Retry-After and keeps the jittered wait."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(503, "30"), make_page("<html>ok</html>")]
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        mock_sleep.assert_called_once_with(5.0)

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_error_429_exhaustion_labels_rate_limiting(self, mock_fetcher, mock_sleep):
        """429 exhaustion keeps legacy wording and adds the label."""
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
        """Non-429 exhaustion messages stay byte-identical."""
        mock_fetcher.get.return_value = _page_with_retry_after(503, "30")
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"), pytest.raises(FetcherError) as excinfo:
            fetcher.fetch(URL, retries=2, backoff=5.0)
        assert str(excinfo.value) == "Blocked with status 503 on http://test.com after 2 attempts"

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_attempt_budget_unchanged(self, mock_fetcher, mock_sleep):
        """Retries still mean the same number of attempts."""
        mock_fetcher.get.return_value = _page_with_retry_after(429, "30")
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"), pytest.raises(FetcherError):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        assert mock_fetcher.get.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_429_records_rate_limit_signal(self, mock_fetcher, mock_sleep):
        """The fast loop records the 429 for the batch cooldown layer."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(429, "30"), make_page("<html>ok</html>")]
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        signal = fetcher._take_rate_limit_signal()
        assert signal is not None
        assert signal.url == URL
        assert 0 < signal.retry_after <= 300
        assert fetcher._take_rate_limit_signal() is None


# ── Batch cooldown primitive ─────────────────────────────────────


class TestGuardsUnchanged:
    """Guards and callback delivery are unchanged."""

    def test_edge_empty_urls_no_fetch(self):
        """Empty URL lists return before any fetch."""
        fetcher = WebFetcher()
        delivered: list[str] = []
        with patch.object(WebFetcher, "fetch") as mock_fetch:
            fetcher.scrape([], lambda url, html: delivered.append(url))
        mock_fetch.assert_not_called()
        assert delivered == []

    def test_error_bad_mode_raises_before_fetch(self):
        """An unsupported mode still raises ValueError first."""
        fetcher = WebFetcher()
        with (
            patch.object(WebFetcher, "fetch") as mock_fetch,
            pytest.raises(ValueError, match="Unsupported scrape mode"),
        ):
            fetcher.scrape([URL], lambda url, html: None, mode="nope")
        mock_fetch.assert_not_called()

    def test_normal_clean_batch_delivers_every_url(self):
        """Every clean URL is delivered exactly once."""
        fetcher = WebFetcher()
        delivered: list[str] = []
        urls = ["http://a.com", "http://b.com", "http://c.com"]
        with patch.object(WebFetcher, "fetch", return_value="<html>clean</html>"):
            fetcher.scrape(urls, lambda url, html: delivered.append(url), max_concurrency=3)
        assert sorted(delivered) == sorted(urls)


# ── Stale-signal and wording regressions ──────────────────────────────────────


class TestEscalationWording:
    """The single-navigation browser path must not render 'after 1 attempts'."""

    def test_normal_escalation_429_message_omits_attempts(self):
        """The browser path message has no attempt clause."""
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
        """The fast path still renders 'after N attempts' byte-identically."""
        fetcher = WebFetcher()
        message = fetcher._blocked_status_message(URL, 429, 3)
        assert message == f"Blocked with status 429 on {URL} after 3 attempts (rate limited)"


# ── status_retries: budgets, Retry-After generality, escalation, cooldown ─────


class TestStatusRetryBudgets:
    """A configured budget overrides the call-time retries argument per status."""

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_configured_budget_overrides_call_time(self, mock_fetcher, mock_sleep):
        """429: 5 with retries=3 gives 6 total attempts for 429."""
        mock_fetcher.get.return_value = _page_with_retry_after(429, None)
        fetcher = WebFetcher(status_retries={429: 5})
        with patch("scrape_kit.fetcher.web_fetcher.logger"), pytest.raises(FetcherError, match="after 6 attempts"):
            fetcher.fetch(URL, retries=3, backoff=0.0)
        assert mock_fetcher.get.call_count == 6
        assert mock_sleep.call_count == 5

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_explicit_zero_delivers_body(self, mock_fetcher, mock_sleep):
        """Budget 0 delivers the body immediately with no retry or wait."""
        mock_fetcher.get.return_value = make_page("<html>unavailable</html>", status=503)
        fetcher = WebFetcher(status_retries={503: 0})
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            result = fetcher.fetch(URL, retries=3, backoff=0.0)
        assert result == "<html>unavailable</html>"
        assert mock_fetcher.get.call_count == 1
        assert mock_sleep.call_count == 0

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_new_code_retries_then_raises_generic_message(self, mock_fetcher, mock_sleep):
        """A new code retries then raises the generic message shape."""
        mock_fetcher.get.return_value = _page_with_retry_after(500, None)
        fetcher = WebFetcher(status_retries={500: 2})
        with patch("scrape_kit.fetcher.web_fetcher.logger"), pytest.raises(FetcherError) as excinfo:
            fetcher.fetch(URL, retries=3, backoff=0.0)
        assert mock_fetcher.get.call_count == 3
        assert str(excinfo.value) == f"Blocked with status 500 on {URL} after 3 attempts"

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_unlisted_default_code_retries(self, mock_fetcher, mock_sleep):
        """An unlisted code keeps its default entry, so 500 retries like its default peers."""
        mock_fetcher.get.return_value = make_page("<html>server error</html>", status=500)
        fetcher = WebFetcher(status_retries={429: 5})
        with patch("scrape_kit.fetcher.web_fetcher.logger"), pytest.raises(FetcherError, match="after 3 attempts"):
            fetcher.fetch(URL, retries=3, backoff=0.0)
        assert mock_fetcher.get.call_count == 3

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_transport_errors_keep_call_time_budget(self, mock_fetcher, mock_sleep):
        """A configured budget never changes the transport-exception budget."""
        mock_fetcher.get.side_effect = OSError("connection reset")
        fetcher = WebFetcher(status_retries={429: 5})
        with patch("scrape_kit.fetcher.web_fetcher.logger"), pytest.raises(FetcherError):
            fetcher.fetch(URL, retries=3, backoff=0.0)
        assert mock_fetcher.get.call_count == 3


class TestStatusRetryAfterGenerality:
    """Retry-After applies to every retryable status, not only 429."""

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_non_429_header_drives_wait(self, mock_fetcher, mock_sleep):
        """A configured 503 honors its Retry-After header."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(503, "30"), make_page("<html>ok</html>")]
        fetcher = WebFetcher(status_retries={503: 3})
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        mock_sleep.assert_called_once_with(30.0)

    @patch("scrape_kit.fetcher.web_fetcher.random.uniform", return_value=1.0)
    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_edge_non_429_absent_header_uses_jitter(self, mock_fetcher, mock_sleep, _jitter):
        """Without a header the existing jittered wait still applies."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(503, None), make_page("<html>ok</html>")]
        fetcher = WebFetcher(status_retries={503: 3})
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        mock_sleep.assert_called_once_with(5.0)

    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_new_code_header_drives_wait(self, mock_fetcher, mock_sleep):
        """A newly configured code also honors Retry-After."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(500, "45"), make_page("<html>ok</html>")]
        fetcher = WebFetcher(status_retries={500: 2})
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        mock_sleep.assert_called_once_with(45.0)


class TestStatusRetryEscalation:
    """The escalation gate follows the same configured map."""

    def _browser_session(self, html, status):
        """Mock browser context manager exposing a cached navigation status."""
        browser_session = MagicMock()
        browser_session.fetch.return_value = html
        browser_session._last_response_status.return_value = status
        browser_session._last_retry_after.return_value = None
        browser_session.__enter__ = MagicMock(return_value=browser_session)
        browser_session.__exit__ = MagicMock(return_value=False)
        return browser_session

    def test_normal_configured_code_not_delivered(self):
        """A configured retryable code is never a successful bypass."""
        fetcher = WebFetcher(status_retries={500: 2})
        browser_session = self._browser_session("<html>error</html>", 500)
        with (
            patch.object(fetcher, "browser", return_value=browser_session),
            pytest.raises(FetcherError, match="Blocked with status 500"),
        ):
            fetcher._escalate_to_browser(URL, "just a moment")

    def test_normal_explicit_zero_delivers(self):
        """An explicit 0 delivers that status on the browser path."""
        fetcher = WebFetcher(status_retries={429: 0})
        browser_session = self._browser_session("<html>slow</html>", 429)
        with patch.object(fetcher, "browser", return_value=browser_session), patch("scrape_kit.fetcher.web_fetcher.logger"):
            assert fetcher._escalate_to_browser(URL, "just a moment") == "<html>slow</html>"

    def test_normal_unconfigured_code_blocked(self):
        """An unconfigured default status is never delivered on the browser path."""
        fetcher = WebFetcher(status_retries={429: 5})
        browser_session = self._browser_session("<html>error</html>", 500)
        with (
            patch.object(fetcher, "browser", return_value=browser_session),
            pytest.raises(FetcherError, match="Blocked with status 500"),
        ):
            fetcher._escalate_to_browser(URL, "just a moment")

    def test_normal_absent_key_403_blocked(self):
        """With no configuration a 403 body is blocked on the browser path."""
        fetcher = WebFetcher()
        browser_session = self._browser_session("<html>forbidden</html>", 403)
        with (
            patch.object(fetcher, "browser", return_value=browser_session),
            pytest.raises(FetcherError, match="Blocked with status 403"),
        ):
            fetcher._escalate_to_browser(URL, "just a moment")

    def test_normal_absent_key_429_still_blocked(self):
        """With no configuration 429 is still never delivered."""
        fetcher = WebFetcher()
        browser_session = self._browser_session("<html>rate limited</html>", 429)
        with patch.object(fetcher, "browser", return_value=browser_session), pytest.raises(FetcherError, match="rate limited"):
            fetcher._escalate_to_browser(URL, "just a moment")

    def test_normal_absent_status_delivered(self):
        """A navigation with no usable status still delivers its body."""
        fetcher = WebFetcher(status_retries={429: 5})
        browser_session = self._browser_session("<html>unknown</html>", None)
        with patch.object(fetcher, "browser", return_value=browser_session):
            assert fetcher._escalate_to_browser(URL, "just a moment") == "<html>unknown</html>"


class TestStatusRetryBackwardCompat:
    """With the key absent the behavior is byte-identical to before."""

    @patch("scrape_kit.fetcher.web_fetcher.random.uniform", return_value=1.0)
    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_absent_key_503_exhaustion_message_unchanged(self, mock_fetcher, mock_sleep, _jitter):
        """The 503 exhaustion message is byte-identical to the baseline."""
        mock_fetcher.get.return_value = _page_with_retry_after(503, "30")
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"), pytest.raises(FetcherError) as excinfo:
            fetcher.fetch(URL, retries=2, backoff=5.0)
        assert str(excinfo.value) == f"Blocked with status 503 on {URL} after 2 attempts"
        mock_sleep.assert_called_once_with(5.0)

    @patch("scrape_kit.fetcher.web_fetcher.random.uniform", return_value=1.0)
    @patch("scrape_kit.fetcher.web_fetcher.time.sleep")
    @patch("scrape_kit.fetcher.web_fetcher.Fetcher")
    def test_normal_absent_key_503_ignores_retry_after(self, mock_fetcher, mock_sleep, _jitter):
        """Without configuration 503 never reads Retry-After."""
        mock_fetcher.get.side_effect = [_page_with_retry_after(503, "30"), make_page("<html>ok</html>")]
        fetcher = WebFetcher()
        with patch("scrape_kit.fetcher.web_fetcher.logger"):
            fetcher.fetch(URL, retries=3, backoff=5.0)
        mock_sleep.assert_called_once_with(5.0)
