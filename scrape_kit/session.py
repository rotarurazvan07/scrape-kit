from typing import Any

from scrapling.fetchers import DynamicSession, StealthySession

from .errors import FetcherError
from .logger import get_logger
from .page import Page

logger = get_logger(__name__)


class InteractiveSession:
    """Wrapper around Scrapling session to provide persistent page and JS execution.

    Isolates browser interaction logic from the main fetcher.
    """

    def __init__(self, session: DynamicSession | StealthySession) -> None:
        self.session = session
        self.page = None
        logger.debug("InteractiveSession initialized with %s", type(session).__name__)

    def __enter__(self) -> "InteractiveSession":
        logger.info("Starting browser session...")
        self.session.start()
        self.page = self.session.context.new_page()
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        try:
            if self.page:
                self.page.close()
        except Exception as e:
            logger.error(f"Error during browser page cleanup: {e}")
        self.session.close()

    def fetch(self, url: str, timeout: int = 90000, wait_until: str = "load") -> Page:
        """Navigate to a URL and return a Page object."""
        if not self.page:
            raise RuntimeError("Session not started. Use 'with browser(...) as session:'")

        logger.info("Browser fetching: %s (timeout=%dms)", url, timeout)
        self.page.goto(url, wait_until=wait_until, timeout=timeout)
        self.wait_for_settle()

        return Page.from_html(self.page.content())

    def wait_for_settle(self, idle_ms: int = 2000, hard_cap_ms: int = 10000) -> None:
        """Wait for page DOM mutations to stop. Reusable after any navigation or action."""
        logger.debug("Waiting for page to settle (idle=%dms, cap=%dms)...", idle_ms, hard_cap_ms)
        try:
            self.execute_script(f"""
                (function() {{
                    return new Promise((resolve) => {{
                        var prevHeight = document.documentElement.scrollHeight;
                        var prevHTML = document.body.innerHTML.length;

                        var timeout = setTimeout(() => {{
                            observer.disconnect();
                            resolve();
                        }}, {hard_cap_ms});

                        var idleTimer = setTimeout(() => {{
                            observer.disconnect();
                            clearTimeout(timeout);
                            resolve();
                        }}, {idle_ms});

                        var observer = new MutationObserver(() => {{
                            var newHeight = document.documentElement.scrollHeight;
                            var newHTML = document.body.innerHTML.length;

                            if (newHeight !== prevHeight || newHTML !== prevHTML) {{
                                prevHeight = newHeight;
                                prevHTML = newHTML;
                                clearTimeout(idleTimer);
                                idleTimer = setTimeout(() => {{
                                    observer.disconnect();
                                    clearTimeout(timeout);
                                    resolve();
                                }}, {idle_ms});
                            }}
                        }});

                        observer.observe(document.body, {{
                            childList: true,
                            subtree: true,
                            attributes: true,
                            characterData: true
                        }});
                    }});
                }})()
            """)
        except Exception as e:
            logger.warning("wait_for_settle script failed (ignoring): %s", e)

    def execute_script(self, script: str) -> Any:
        """Execute JavaScript in the page context."""
        if not self.page:
            raise RuntimeError("Browser page not initialized")

        clean_script = script.strip()
        try:
            if clean_script.startswith("return "):
                return self.page.evaluate(f"() => {{ {clean_script} }}")
            return self.page.evaluate(script)
        except Exception as e:
            logger.error("Script execution error: %s", e)
            raise

    def click(
        self,
        selector: str,
        text: str | None = None,
        visible_only: bool = False,
        idle_ms: int = 2000,
    ) -> bool:
        """Click an element and wait for the page to settle."""
        logger.debug("Clicking %s (text=%s)...", selector, text)
        success = self.execute_script(f"""
            (function() {{
                const els = Array.from(document.querySelectorAll("{selector}"));
                const target = "{text or ''}" 
                    ? els.find(e => e.textContent.includes("{text or ''}")) 
                    : els[0];
                
                if (!target) return false;
                if ({str(visible_only).lower()} && target.offsetParent === null) return false;
                
                target.click();
                return true;
            }})()
        """)
        if success:
            self.wait_for_settle(idle_ms=idle_ms)
        return success

    def scroll_to_bottom(self, infinite: bool = True, idle_ms: int = 2000) -> None:
        """Scroll to the bottom of the page, optionally waiting for new content to load."""
        logger.debug("Scrolling to bottom (infinite=%s)...", infinite)
        self.execute_script(f"""
            (async () => {{
                let lastHeight = document.body.scrollHeight;
                while (true) {{
                    window.scrollTo(0, document.body.scrollHeight);
                    await new Promise(r => setTimeout(r, 1000));
                    let newHeight = document.body.scrollHeight;
                    if (newHeight === lastHeight) break;
                    lastHeight = newHeight;
                    if (!{str(infinite).lower()}) break;
                }}
            }})()
        """)
        self.wait_for_settle(idle_ms=idle_ms)

    def wait_for_selector(self, selector: str, timeout: int = 30000, **kwargs: Any) -> None:
        if not self.page:
            raise RuntimeError("Browser page not initialized")
        self.page.wait_for_selector(selector, timeout=timeout, **kwargs)

    def wait_for_function(self, expression: str, timeout: int = 30000, **kwargs: Any) -> None:
        if not self.page:
            raise RuntimeError("Browser page not initialized")
        self.page.wait_for_function(expression, timeout=timeout, **kwargs)

    def wait_for_timeout(self, ms: int, **kwargs: Any) -> None:
        if not self.page:
            raise RuntimeError("Browser page not initialized")
        self.page.wait_for_timeout(ms, **kwargs)

    def __getattr__(self, name: str) -> Any:
        """Delegate to the underlying Scrapling session."""
        return getattr(self.session, name)
