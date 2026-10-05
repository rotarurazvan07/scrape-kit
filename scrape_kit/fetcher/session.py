"""InteractiveSession: persistent browser page wrapper with JS execution around scrapling."""

import json
import time
from typing import Any

from scrapling.fetchers import DynamicSession, StealthySession

from ..errors import FetcherError
from ..logger import get_logger

logger = get_logger(__name__)


class InteractiveSession:
    """Wrapper around a Scrapling session to provide a persistent page and JS execution.

    Usage:

        with WebFetcher.browser() as session:
            html = session.fetch("https://example.com")
            session.click(".button", text="Submit")

    The session must be started via the ``with`` statement (or ``__enter__``)
    before any page interaction; state violations raise FetcherError, so a single
    ``except ScrapeKitError`` covers every domain failure.
    """

    def __init__(self, session: DynamicSession | StealthySession) -> None:
        """Initialize an interactive session with a Scrapling session.

        Args:
            session: A Scrapling DynamicSession or StealthySession instance.
        """
        self.session = session
        self.page: Any = None
        logger.debug("InteractiveSession initialized with %s", type(session).__name__)

    def __enter__(self) -> "InteractiveSession":
        """Start the browser session and create a new page.

        Returns:
            The InteractiveSession instance for use in a context manager.
        """
        logger.info("Starting browser session...")
        self.session.start()
        self.page = self.session.context.new_page()
        logger.debug("Browser page created and session started")
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        """Close the browser page, then always close the underlying session.

        The session is closed in a finally block, so a page-close failure can
        never leak the browser process. The cleanup FetcherError is raised only
        when the with-body itself raised nothing, so body exceptions are never
        masked by cleanup noise.

        Args:
            _exc_type: Exception type if an exception occurred in the body.
            _exc_val: Exception value if an exception occurred in the body.
            _exc_tb: Traceback if an exception occurred in the body.

        Raises:
            FetcherError: If closing the page fails while no body exception is active.
        """
        try:
            if self.page:
                logger.debug("Closing browser page...")
                self.page.close()
        except Exception as e:
            if _exc_type is None:
                raise FetcherError(f"Cleanup failed: {e}") from e
            # Body already raised: log the cleanup failure, never mask the original.
            logger.error("Cleanup failed while handling %s: %s", _exc_val, e)
        finally:
            logger.info("Closing browser session")
            # scrapling ships no type stubs, so close() is untyped at the call site.
            self.session.close()  # type: ignore[no-untyped-call]

    def fetch(self, url: str, timeout: int = 90000, wait_until: str = "load") -> str:
        """Navigate to a URL and return the settled page HTML.

        Args:
            url: The URL to fetch.
            timeout: Navigation timeout in milliseconds. Defaults to 90000.
            wait_until: Page load state to wait for. Defaults to "load".

        Returns:
            The page HTML as a string.

        Raises:
            FetcherError: If the session has not been started.
        """
        if not self.page:
            raise FetcherError("Session not started. Use 'with WebFetcher.browser(...) as session:'")
        logger.info("Browser fetching: %s (timeout=%dms)", url, timeout)
        self.page.goto(url, wait_until=wait_until, timeout=timeout)
        logger.debug("Waiting for site to settle ...")
        max_wait = 30  # maximum seconds to keep trying
        start_time = time.time()

        while True:
            try:
                self.execute_script(
                    """
                    (function() {
                        return new Promise((resolve) => {
                            var prevHeight = document.documentElement.scrollHeight;
                            var prevHTML = document.body.innerHTML.length;

                            var timeout = setTimeout(() => {
                                observer.disconnect();
                                resolve();
                            }, 10000);  // hard cap

                            var idleTimer = setTimeout(() => {
                                observer.disconnect();
                                clearTimeout(timeout);
                                resolve();
                            }, 2000);  // resolve after 2s of no changes

                            var observer = new MutationObserver(() => {
                                var newHeight = document.documentElement.scrollHeight;
                                var newHTML = document.body.innerHTML.length;

                                if (newHeight !== prevHeight || newHTML !== prevHTML) {
                                    prevHeight = newHeight;
                                    prevHTML = newHTML;
                                    clearTimeout(idleTimer);
                                    idleTimer = setTimeout(() => {
                                        observer.disconnect();
                                        clearTimeout(timeout);
                                        resolve();
                                    }, 2000);
                                }
                            });

                            observer.observe(document.body, {
                                childList: true,
                                subtree: true,
                                attributes: true,
                                characterData: true
                            });
                        });
                    })()
                """
                )
                break  # success, exit loop

            except Exception as e:
                elapsed = time.time() - start_time
                if elapsed >= max_wait:
                    logger.warning(f"Page settle script failed after {max_wait}s, giving up: {e}")
                    break
                logger.debug(f"Page settle script failed ({e}), retrying in 1s...")
                time.sleep(1)
        content: str = self.page.content()
        logger.debug("Fetch complete, content length: %d", len(content))
        return content

    def execute_script(self, script: str) -> Any:
        """Execute JavaScript in the page context.

        Args:
            script: JavaScript code to execute. If it starts with "return ", the
                    expression is wrapped in an arrow function and its result is returned.

        Returns:
            The result of the script execution, if any.

        Raises:
            FetcherError: If fetch() has not been called first.
        """
        if not self.page:
            raise FetcherError("Call fetch() first")
        clean_script = script.strip()
        logger.debug("Executing script: %s...", clean_script[:50])
        try:
            if clean_script.startswith("return "):
                return self.page.evaluate(f"() => {{ {clean_script} }}")
            return self.page.evaluate(script)
        except Exception as e:
            logger.error(f"Script Execution Error: {e}")
            raise

    def wait_for_selector(self, selector: str, timeout: int = 30000, **kwargs: Any) -> None:
        """Wait for a DOM element to appear.

        Args:
            selector: CSS selector to wait for.
            timeout: Maximum wait time in milliseconds. Defaults to 30000.
            **kwargs: Additional arguments passed to Playwright's wait_for_selector.

        Raises:
            FetcherError: If fetch() has not been called first.
        """
        if not self.page:
            raise FetcherError("Call fetch() first")
        self.page.wait_for_selector(selector, timeout=timeout, **kwargs)

    def wait_for_function(self, expression: str, timeout: int = 30000, **kwargs: Any) -> None:
        """Wait for a JavaScript function to return a truthy value.

        Args:
            expression: JavaScript function or expression to evaluate.
            timeout: Maximum wait time in milliseconds. Defaults to 30000.
            **kwargs: Additional arguments passed to Playwright's wait_for_function.

        Raises:
            FetcherError: If fetch() has not been called first.
        """
        if not self.page:
            raise FetcherError("Call fetch() first")
        self.page.wait_for_function(expression, timeout=timeout, **kwargs)

    def wait_for_timeout(self, ms: int, **kwargs: Any) -> None:
        """Pause execution for a specified duration.

        Args:
            ms: Time to wait in milliseconds.
            **kwargs: Additional arguments passed to Playwright's wait_for_timeout.

        Raises:
            FetcherError: If fetch() has not been called first.
        """
        if not self.page:
            raise FetcherError("Call fetch() first")
        self.page.wait_for_timeout(ms, **kwargs)

    def scroll_to_bottom(self, infinite: bool = True, idle_ms: int = 10000, cycle_delay_ms: int | None = None) -> None:
        """Scroll to the page bottom, optionally looping until content stops growing.

        Args:
            infinite: Keep scrolling while new content appears. Defaults to True.
            idle_ms: Milliseconds to wait for new content before settling. Defaults to 10000.
            cycle_delay_ms: Delay between scroll cycles. Defaults to idle_ms // 5.

        Raises:
            FetcherError: If fetch() has not been called first.
        """
        if not self.page:
            raise FetcherError("Call fetch() first")
        cycle_delay_ms = cycle_delay_ms if cycle_delay_ms is not None else idle_ms // 5
        self.execute_script(
            f"""
            (function() {{
                return new Promise((resolve) => {{
                    var infinite = {"true" if infinite else "false"};
                    var idle_ms = {idle_ms};
                    var cycle_delay_ms = {cycle_delay_ms};
                    function cycle() {{
                        var maxScroll = Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);
                        document.body.scrollTop = maxScroll;
                        document.documentElement.scrollTop = maxScroll;
                        var prevHeight = maxScroll;
                        var idleTimeout = setTimeout(() => {{ observer.disconnect(); resolve(); }}, idle_ms);
                        var observer = new MutationObserver(() => {{
                            var newHeight = Math.max(document.body.scrollHeight, document.documentElement.scrollHeight);
                            if (newHeight > prevHeight) {{
                                clearTimeout(idleTimeout);
                                observer.disconnect();
                                infinite ? setTimeout(cycle, cycle_delay_ms) : resolve();
                            }}
                        }});
                        observer.observe(document.body, {{childList: true, subtree: true}});
                    }}
                    cycle();
                }});
            }})()
        """
        )

    def click(
        self,
        selector: str,
        text: str | None = None,
        visible_only: bool = False,
        idle_ms: int = 5000,
        hard_cap_ms: int | None = None,
    ) -> Any:
        """Click an element matching the selector (optionally by text) and wait for DOM changes to settle.

        Selector and text are JSON-escaped before splicing into the script, so
        values containing quotes or backslashes click correctly and cannot
        inject JavaScript.

        Args:
            selector: CSS selector of the element(s) to click.
            text: If given, click the first matching element whose trimmed
                    textContent equals this value. Defaults to None.
            visible_only: Only click elements that are visible. Defaults to False.
            idle_ms: Milliseconds of DOM quiet before resolving. Defaults to 5000.
            hard_cap_ms: Absolute resolve deadline. Defaults to idle_ms * 6.

        Returns:
            True if the element was clicked and the DOM settled; False if nothing
            matched the selector/text, the element was hidden under visible_only,
            or the script failed.

        Raises:
            FetcherError: If fetch() has not been called first.
        """
        if not self.page:
            raise FetcherError("Call fetch() first")
        hard_cap_ms = hard_cap_ms if hard_cap_ms is not None else idle_ms * 6
        return bool(
            self.execute_script(
                f"""
            (function() {{
                return new Promise((resolve) => {{
                    try {{
                        var idle_ms = {idle_ms};
                        var hard_cap_ms = {hard_cap_ms};

                        function isVisible(el) {{
                            if (!el) return false;
                            var s = window.getComputedStyle(el);
                            return s.display !== 'none' && s.visibility !== 'hidden'
                                && s.opacity !== '0' && el.offsetWidth > 0 && el.offsetHeight > 0;
                        }}

                        var el;
                        if ({"true" if text else "false"}) {{
                            el = Array.from(document.querySelectorAll({json.dumps(selector)}))
                                .find(e => e.textContent.trim() === {json.dumps(str(text))});
                        }} else {{
                            el = document.querySelector({json.dumps(selector)});
                        }}

                        if (!el) {{ resolve(false); return; }}
                        if ({"true" if visible_only else "false"} && !isVisible(el)) {{ resolve(false); return; }}

                        el.dispatchEvent(new MouseEvent('click', {{ bubbles: true, cancelable: true }}));

                        var prevHeight = document.documentElement.scrollHeight;
                        var prevHTML = document.body.innerHTML.length;
                        var observer = null;

                        var hardCap = setTimeout(() => {{ observer && observer.disconnect(); resolve(true); }}, hard_cap_ms);
                        var idleTimer = setTimeout(() => {{
                            observer && observer.disconnect(); clearTimeout(hardCap); resolve(true);
                        }}, idle_ms);

                        observer = new MutationObserver(() => {{
                            var newHeight = document.documentElement.scrollHeight;
                            var newHTML = document.body.innerHTML.length;
                            if (newHeight !== prevHeight || newHTML !== prevHTML) {{
                                prevHeight = newHeight;
                                prevHTML = newHTML;
                                clearTimeout(idleTimer);
                                idleTimer = setTimeout(() => {{
                                    observer && observer.disconnect(); clearTimeout(hardCap); resolve(true);
                                }}, idle_ms);
                            }}
                        }});

                        observer.observe(document.body, {{
                            childList: true, subtree: true, attributes: true, characterData: true
                        }});

                    }} catch(e) {{ resolve(false); }}
                }});
            }})()
            """
            )
        )

    def __getattr__(self, name: str) -> Any:
        """Delegate unknown attributes to the wrapped scrapling session.

        Args:
            name: Attribute name to look up on the wrapped session.

        Returns:
            The attribute value from the wrapped session.
        """
        return getattr(self.session, name)
