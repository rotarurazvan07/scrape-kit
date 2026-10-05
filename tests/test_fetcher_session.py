"""InteractiveSession."""

import json
import re
from unittest.mock import MagicMock, patch

import pytest
from conftest import (
    CLICK_HARD_CAP_DEFAULT_MS,
    CLICK_HARD_CAP_MS,
    CLICK_IDLE_ALT_MS,
    CLICK_IDLE_MS,
    WAIT_FUNCTION_MS,
    WAIT_SELECTOR_MS,
    WAIT_TIMEOUT_MS,
    make_interactive_session,
)

from scrape_kit.errors import FetcherError
from scrape_kit.fetcher.session import InteractiveSession

pytestmark = pytest.mark.p0


# ── InteractiveSession ────────────────────────────────────────────────────────


class TestInteractiveSessionContextManager:
    """InteractiveSession enter/exit lifecycle incl. page-close error path."""

    def test_normal_enter_starts_session_and_creates_page(self):
        mock_session, mock_page = make_interactive_session()
        with InteractiveSession(mock_session) as session:
            mock_session.start.assert_called_once()
            assert session.page is mock_page

    def test_normal_exit_closes_page_and_session(self):
        mock_session, mock_page = make_interactive_session()
        with InteractiveSession(mock_session):
            pass
        mock_page.close.assert_called_once()
        mock_session.close.assert_called_once()

    def test_edge_page_close_exception_raises_but_session_still_closed(self):
        mock_session, mock_page = make_interactive_session()
        mock_page.close.side_effect = RuntimeError("browser crash")
        with pytest.raises(FetcherError), InteractiveSession(mock_session):
            pass
        # __exit__ must close the session even when page.close() failed first.
        mock_session.close.assert_called_once()

    def test_edge_body_exception_not_masked_session_still_closed(self):
        mock_session, mock_page = make_interactive_session()
        with (
            pytest.raises(ValueError, match="body boom"),
            InteractiveSession(mock_session),
        ):
            raise ValueError("body boom")
        # Cleanup always runs; the body exception is never masked.
        mock_page.close.assert_called_once()
        mock_session.close.assert_called_once()


class TestInteractiveSessionFetch:
    """InteractiveSession.fetch returns page content and forwards timeout args."""

    def test_normal_fetch_returns_namespace_with_html(self):
        mock_session, mock_page = make_interactive_session("<html>loaded</html>")
        session = InteractiveSession(mock_session)
        session.__enter__()
        result = session.fetch("http://example.com")
        assert isinstance(result, str)
        assert result == "<html>loaded</html>"
        mock_page.goto.assert_called_once()
        # Post-rework flow (issue #3): the post-navigation settle wait runs as
        # an injected MutationObserver script — wait_for_timeout is never used.
        mock_page.wait_for_timeout.assert_not_called()
        mock_page.evaluate.assert_called_once()

    def test_edge_fetch_without_enter_raises_fetcher_error(self):
        mock_session = MagicMock()
        session = InteractiveSession(mock_session)
        with pytest.raises(FetcherError, match="Session not started"):
            session.fetch("http://example.com")

    def test_normal_fetch_passes_timeout_and_wait_until(self):
        mock_session, mock_page = make_interactive_session()
        session = InteractiveSession(mock_session)
        session.__enter__()
        session.fetch("http://example.com", timeout=5000, wait_until="networkidle")
        mock_page.goto.assert_called_once_with("http://example.com", wait_until="networkidle", timeout=5000)

    def test_error_settle_never_idle_gives_up_bounded(self):
        """A page that never settles ends in a bounded give-up after max_wait, not a hang."""
        mock_session, mock_page = make_interactive_session("<html>partial</html>")
        mock_page.evaluate.side_effect = Exception("never settles")
        session = InteractiveSession(mock_session)
        session.__enter__()
        with (
            patch("scrape_kit.fetcher.session.logger") as mock_log,
            patch("time.time", side_effect=[0, 10, 20, 30]),
            patch("time.sleep") as mock_sleep,
        ):
            result = session.fetch("http://example.com")
        assert result == "<html>partial</html>"
        assert mock_page.evaluate.call_count == 3  # two retries, then the give-up attempt
        assert mock_sleep.call_count == 2
        assert mock_log.warning.call_count == 1
        assert "giving up" in str(mock_log.warning.call_args)


class TestInteractiveSessionExecuteScript:
    """InteractiveSession.execute_script evaluation and error propagation."""

    def test_normal_plain_script_evaluates_directly(self):
        mock_session, mock_page = make_interactive_session(eval_return="test-title")
        session = InteractiveSession(mock_session)
        session.__enter__()
        result = session.execute_script("document.title")
        mock_page.evaluate.assert_called_once_with("document.title")
        assert result == "test-title"

    def test_normal_return_prefix_wrapped_in_arrow_function(self):
        mock_session, mock_page = make_interactive_session(eval_return=42)
        session = InteractiveSession(mock_session)
        session.__enter__()
        result = session.execute_script("return document.title.length")
        mock_page.evaluate.assert_called_once_with("() => { return document.title.length }")
        assert result == 42

    def test_edge_execute_without_enter_raises(self):
        session = InteractiveSession(MagicMock())
        with pytest.raises(FetcherError, match="Call fetch"):
            session.execute_script("1 + 1")

    def test_error_js_error_propagates(self):
        mock_session, mock_page = make_interactive_session()
        mock_page.evaluate.side_effect = Exception("ReferenceError: x is not defined")
        session = InteractiveSession(mock_session)
        session.__enter__()
        with pytest.raises(Exception, match="ReferenceError"):
            session.execute_script("undeclared_var()")


class TestInteractiveSessionHelpers:
    """InteractiveSession delegation helpers raise before __enter__ and pass through after."""

    def _started(self, mock_session, mock_page):
        session = InteractiveSession(mock_session)
        session.__enter__()
        return session

    def test_normal_wait_for_selector_delegates(self):
        mock_session, mock_page = make_interactive_session()
        session = self._started(mock_session, mock_page)
        session.wait_for_selector("#submit", timeout=WAIT_SELECTOR_MS)
        mock_page.wait_for_selector.assert_called_once_with("#submit", timeout=WAIT_SELECTOR_MS)

    def test_normal_wait_for_function_delegates(self):
        mock_session, mock_page = make_interactive_session()
        session = self._started(mock_session, mock_page)
        session.wait_for_function("() => window.ready", timeout=WAIT_FUNCTION_MS)
        mock_page.wait_for_function.assert_called_once_with("() => window.ready", timeout=WAIT_FUNCTION_MS)

    def test_normal_click_embeds_idle_and_hard_cap_in_js(self):
        mock_session, mock_page = make_interactive_session()
        session = self._started(mock_session, mock_page)
        # hard_cap ≠ 6×idle (the default) — proves the explicit override is honored.
        session.click(".btn", idle_ms=CLICK_IDLE_MS, hard_cap_ms=CLICK_HARD_CAP_MS)
        # Post-rework contract (issue #3): click() injects a MutationObserver
        # settle script through page.evaluate — page.click is never used.
        mock_page.click.assert_not_called()
        script = mock_page.evaluate.call_args[0][0]
        assert f"var idle_ms = {CLICK_IDLE_MS};" in script
        assert f"var hard_cap_ms = {CLICK_HARD_CAP_MS};" in script
        # The script must actually locate and dispatch a click on the target.
        # Selector is JSON-escaped, so it appears with double quotes.
        assert 'document.querySelector(".btn")' in script
        assert "dispatchEvent(new MouseEvent('click'" in script

    def test_normal_click_escapes_quotes_in_selector_and_text(self):
        """Selector/text are JSON-escaped before splicing into the click JS."""
        mock_session, mock_page = make_interactive_session()
        session = self._started(mock_session, mock_page)
        selector = 'a[href*="x"]'
        text = "O'Brien"
        session.click(selector, text=text)
        script = mock_page.evaluate.call_args[0][0]
        assert f"document.querySelectorAll({json.dumps(selector)})" in script
        assert f"=== {json.dumps(text)}" in script
        # Raw single-quoted splicing is gone — quotes cannot break out of the literal.
        assert f"querySelectorAll('{selector}')" not in script

    def test_normal_click_hard_cap_defaults_to_six_times_idle(self):
        mock_session, mock_page = make_interactive_session()
        session = self._started(mock_session, mock_page)
        session.click(".btn", idle_ms=CLICK_IDLE_ALT_MS)
        script = mock_page.evaluate.call_args[0][0]
        assert f"var idle_ms = {CLICK_IDLE_ALT_MS};" in script
        assert f"var hard_cap_ms = {CLICK_HARD_CAP_DEFAULT_MS};" in script  # default = 6 × idle_ms

    def test_normal_wait_for_timeout_delegates(self):
        mock_session, mock_page = make_interactive_session()
        session = self._started(mock_session, mock_page)
        session.wait_for_timeout(WAIT_TIMEOUT_MS)
        mock_page.wait_for_timeout.assert_called_with(WAIT_TIMEOUT_MS)

    def test_edge_getattr_delegates_to_underlying_session(self):
        mock_session = MagicMock()
        mock_session.cookies = {"session": "abc"}
        session = InteractiveSession(mock_session)
        assert session.cookies == {"session": "abc"}

    def test_edge_helpers_without_enter_raise_fetcher_error(self):
        session = InteractiveSession(MagicMock())
        for method, args in [
            ("wait_for_selector", ("#x",)),
            ("wait_for_function", ("() => true",)),
            ("click", (".btn",)),
            ("wait_for_timeout", (1000,)),
        ]:
            with pytest.raises(FetcherError):
                getattr(session, method)(*args)


class TestScrollToBottom:
    """scroll_to_bottom embeds its scroll-loop JS and guards on session start (#7)."""

    def test_normal_embeds_scroll_loop_js(self):
        mock_session, mock_page = make_interactive_session()
        session = InteractiveSession(mock_session)
        session.__enter__()
        session.scroll_to_bottom(infinite=True, idle_ms=CLICK_IDLE_ALT_MS)
        script = mock_page.evaluate.call_args[0][0]
        assert "var infinite = true;" in script
        assert f"var idle_ms = {CLICK_IDLE_ALT_MS};" in script
        assert "MutationObserver" in script  # the settle-detection primitive

    def test_normal_cycle_delay_defaults_to_idle_over_five(self):
        mock_session, mock_page = make_interactive_session()
        session = InteractiveSession(mock_session)
        session.__enter__()
        session.scroll_to_bottom(idle_ms=CLICK_IDLE_ALT_MS)  # no explicit cycle delay
        script = mock_page.evaluate.call_args[0][0]
        assert f"var cycle_delay_ms = {CLICK_IDLE_ALT_MS // 5};" in script  # default = idle_ms // 5

    def test_normal_explicit_cycle_delay_overrides_default(self):
        mock_session, mock_page = make_interactive_session()
        session = InteractiveSession(mock_session)
        session.__enter__()
        session.scroll_to_bottom(idle_ms=CLICK_IDLE_ALT_MS, cycle_delay_ms=250)
        script = mock_page.evaluate.call_args[0][0]
        assert "var cycle_delay_ms = 250;" in script

    def test_normal_finite_mode_disables_recycle(self):
        mock_session, mock_page = make_interactive_session()
        session = InteractiveSession(mock_session)
        session.__enter__()
        session.scroll_to_bottom(infinite=False)
        script = mock_page.evaluate.call_args[0][0]
        assert "var infinite = false;" in script

    def test_error_before_enter_raises_fetcher_error(self):
        session = InteractiveSession(MagicMock())
        with pytest.raises(FetcherError, match=re.escape("Call fetch() first")):
            session.scroll_to_bottom()
