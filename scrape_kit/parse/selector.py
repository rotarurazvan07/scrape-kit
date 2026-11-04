"""Selector: thin 1:1 facade over scrapling's parser."""

from __future__ import annotations

from typing import Any, Callable, Iterator, List, Optional, Pattern, Union

from ..errors import FetcherError


class Selector:
    """Thin facade over scrapling's ``Selector``.

    All exceptions are converted to :class:`FetcherError` with the source URL
    attached and the original exception chained as ``__cause__``. The wrapped
    scrapling object is never exposed directly.
    """

    def __init__(
        self,
        html: Union[str, bytes],
        url: str = "",
        encoding: str = "utf-8",
        adaptive: bool = False,
        adaptive_domain: Optional[str] = None,
    ) -> None:
        """Create a selector from HTML content.

        Args:
            html: HTML string or bytes to parse.
            url: Base URL for relative link resolution and adaptive storage.
            encoding: Encoding used when ``html`` is bytes.
            adaptive: Enable scrapling adaptive element relocation.
            adaptive_domain: Domain key for adaptive storage isolation.

        Raises:
            FetcherError: If the parser is unavailable or construction fails.
            ValueError: If html is empty.
        """
        if not html:
            raise ValueError("html must not be empty")
        try:
            from scrapling.parser import Selector as ScraplingSelector
        except Exception as e:  # pragma: no cover - import-time only
            raise FetcherError("scrapling parser unavailable", url=url) from e

        try:
            self._selector: Any = ScraplingSelector(
                content=html,
                url=url,
                encoding=encoding,
                adaptive=adaptive,
                adaptive_domain=adaptive_domain,
            )
        except Exception as e:
            raise FetcherError("failed to create selector", url=url) from e

        self._url = url

    @property
    def url(self) -> str:
        """Return the base URL of the parsed document."""
        return self._url

    @property
    def first(self) -> "Selector":
        """Return the first matched element as a Selector.

        Note: Scrapling's Selector does not expose ``first``/``last``; this
        property delegates to the internal selector if available, which keeps
        the API symmetric with Selectors.

        Returns:
            Selector for the first element.

        Raises:
            FetcherError: If extraction fails.
        """
        try:
            return Selector._wrap_single(self._selector.first, self._url)
        except Exception as e:
            raise FetcherError("first failed", url=self._url) from e

    @property
    def last(self) -> "Selector":
        """Return the last matched element as a Selector.

        Note: Scrapling's Selector does not expose ``first``/``last``; this
        property delegates to the internal selector if available.

        Returns:
            Selector for the last element.

        Raises:
            FetcherError: If extraction fails.
        """
        try:
            return Selector._wrap_single(self._selector.last, self._url)
        except Exception as e:
            raise FetcherError("last failed", url=self._url) from e

    def markdown(self, **kwargs: Any) -> str:
        """Extract markdown from the selected element.

        Note: Requires the ``scrapling[rag]`` extra, which is not a dependency
        of scrape-kit. Raises NotImplementedError when the extra is absent.

        Args:
            **kwargs: Additional arguments passed to scrapling's markdown method.

        Returns:
            Markdown string representation.

        Raises:
            NotImplementedError: If the ``scrapling[rag]`` extra is not installed.
            FetcherError: If markdown extraction fails.
        """
        try:
            return str(self._selector.markdown(**kwargs))
        except AttributeError as e:
            raise NotImplementedError(
                "markdown() requires scrapling[rag] extra. Install with: pip install 'scrapling[rag]'"
            ) from e
        except Exception as e:
            raise FetcherError("markdown extraction failed", url=self._url) from e

    def css(
        self,
        selector: str,
        adaptive: bool = False,
        auto_save: bool = False,
        percentage: int = 40,
    ) -> "Selectors":
        """Select elements with a CSS selector.

        Args:
            selector: CSS selector expression.
            adaptive: Enable scrapling adaptive relocation for this selection.
            auto_save: Persist matched fingerprints when adaptive is enabled.
            percentage: Similarity threshold percentage for adaptive relocation.

        Returns:
            Selectors of matched elements.

        Raises:
            FetcherError: If selection fails.
            ValueError: If the selector is empty or percentage is out of range.
        """
        if not selector:
            raise ValueError("selector must not be empty")
        if not 0 <= percentage <= 100:
            raise ValueError("percentage must be between 0 and 100")
        try:
            return Selectors(
                self._selector.css(
                    selector,
                    adaptive=adaptive,
                    auto_save=auto_save,
                    percentage=percentage,
                ),
                self._url,
            )
        except Exception as e:
            raise FetcherError(f"css selection failed: {selector}", url=self._url) from e

    def xpath(
        self,
        selector: str,
        adaptive: bool = False,
        auto_save: bool = False,
        percentage: int = 40,
    ) -> "Selectors":
        """Select elements with an XPath selector.

        Args:
            selector: XPath selector expression.
            adaptive: Enable scrapling adaptive relocation for this selection.
            auto_save: Persist matched fingerprints when adaptive is enabled.
            percentage: Similarity threshold percentage for adaptive relocation.

        Returns:
            Selectors of matched elements.

        Raises:
            FetcherError: If selection fails.
            ValueError: If the selector is empty or percentage is out of range.
        """
        if not selector:
            raise ValueError("selector must not be empty")
        if not 0 <= percentage <= 100:
            raise ValueError("percentage must be between 0 and 100")
        try:
            return Selectors(
                self._selector.xpath(
                    selector,
                    adaptive=adaptive,
                    auto_save=auto_save,
                    percentage=percentage,
                ),
                self._url,
            )
        except Exception as e:
            raise FetcherError(f"xpath selection failed: {selector}", url=self._url) from e

    def find_by_text(
        self,
        text: str,
        first_match: bool = True,
        partial: bool = False,
        case_sensitive: bool = False,
        clean_match: bool = True,
    ) -> Union["Selector", "Selectors"]:
        """Find elements whose text matches ``text``.

        Args:
            text: Text to search for.
            first_match: Return only the first match when True.
            partial: Allow partial text matches.
            case_sensitive: Match case when True.
            clean_match: Strip whitespace before matching.

        Returns:
            A Selector (first match) or Selectors.

        Raises:
            FetcherError: If the search fails.
        """
        try:
            if first_match:
                single = self._selector.find_by_text(text, True, partial, case_sensitive, clean_match)
                return Selector._wrap_single(single, self._url)
            many = self._selector.find_by_text(text, False, partial, case_sensitive, clean_match)
            return Selectors(many, self._url)
        except Exception as e:
            raise FetcherError(f"find_by_text failed: {text}", url=self._url) from e

    def find_by_regex(
        self,
        query: Union[str, Pattern[str]],
        first_match: bool = True,
        case_sensitive: bool = False,
        clean_match: bool = True,
    ) -> Union["Selector", "Selectors"]:
        """Find elements matching a regular expression.

        Args:
            query: Regex string or compiled pattern.
            first_match: Return only the first match when True.
            case_sensitive: Match case when True.
            clean_match: Strip whitespace before matching.

        Returns:
            A Selector (first match) or Selectors.

        Raises:
            FetcherError: If the search fails.
        """
        try:
            if first_match:
                single = self._selector.find_by_regex(query, True, case_sensitive, clean_match)
                return Selector._wrap_single(single, self._url)
            many = self._selector.find_by_regex(query, False, case_sensitive, clean_match)
            return Selectors(many, self._url)
        except Exception as e:
            raise FetcherError("find_by_regex failed", url=self._url) from e

    def find_similar(
        self,
        similarity_threshold: float = 0.2,
        ignore_attributes: Union[List[str], tuple[str, ...]] = ("href", "src"),
        match_text: bool = False,
    ) -> "Selectors":
        """Find elements structurally similar to the first matched element.

        Args:
            similarity_threshold: Minimum similarity ratio.
            ignore_attributes: Attributes excluded from comparison.
            match_text: Include text content in comparison.

        Returns:
            Selectors of similar elements.

        Raises:
            FetcherError: If the search fails.
        """
        try:
            result = self._selector.find_similar(similarity_threshold, ignore_attributes, match_text)
            return Selectors(result, self._url)
        except Exception as e:
            raise FetcherError("find_similar failed", url=self._url) from e

    def get(self) -> Any:
        """Return the text of the first matched element.

        Returns:
            The element text.

        Raises:
            FetcherError: If extraction fails.
        """
        try:
            return self._selector.get()
        except Exception as e:
            raise FetcherError("get failed", url=self._url) from e

    def getall(self) -> List[str]:
        """Return the text of all matched elements.

        Returns:
            A list of element texts.

        Raises:
            FetcherError: If extraction fails.
        """
        try:
            return [str(item) for item in self._selector.getall()]
        except Exception as e:
            raise FetcherError("getall failed", url=self._url) from e

    def re(self, regex: Union[str, Pattern[str]], **kwargs: Any) -> List[str]:
        """Apply a regex to the matched element text.

        Args:
            regex: Regex string or compiled pattern.
            **kwargs: Forwarded to scrapling's ``re``.

        Returns:
            A list of regex results.

        Raises:
            FetcherError: If the regex application fails.
        """
        try:
            return [str(item) for item in self._selector.re(regex, **kwargs)]
        except Exception as e:
            raise FetcherError(f"re failed: {regex}", url=self._url) from e

    def re_first(self, regex: Union[str, Pattern[str]], default: Any = None, **kwargs: Any) -> Any:
        """Apply a regex and return the first result.

        Args:
            regex: Regex string or compiled pattern.
            default: Value returned when nothing matches.
            **kwargs: Forwarded to scrapling's ``re_first``.

        Returns:
            The first regex result, or ``default``.

        Raises:
            FetcherError: If the regex application fails.
        """
        try:
            return self._selector.re_first(regex, default, **kwargs)
        except Exception as e:
            raise FetcherError(f"re_first failed: {regex}", url=self._url) from e

    def save(self, element: Any, identifier: str) -> None:
        """Persist an element fingerprint for future relocation.

        Args:
            element: The element to remember.
            identifier: Key used to retrieve the fingerprint later.

        Raises:
            FetcherError: If saving fails.
        """
        try:
            target = element._selector if isinstance(element, Selector) else element
            self._selector.save(target, identifier)
        except Exception as e:
            raise FetcherError(f"adaptive save failed: {identifier}", url=self._url) from e

    def retrieve(self, identifier: str) -> Optional[dict[str, Any]]:
        """Return a previously saved element fingerprint.

        Args:
            identifier: Key used when the element was saved.

        Returns:
            The stored fingerprint, or None when absent.

        Raises:
            FetcherError: If retrieval fails.
        """
        try:
            result: Any = self._selector.retrieve(identifier)
            return None if result is None else dict(result)
        except Exception as e:
            raise FetcherError(f"adaptive retrieve failed: {identifier}", url=self._url) from e

    def relocate(
        self,
        element: Any,
        percentage: int = 40,
        selector_type: bool = False,
    ) -> "Selectors":
        """Relocate a saved element in the current document.

        Args:
            element: A stored fingerprint, an element, or a Selector.
            percentage: Similarity threshold percentage.
            selector_type: Return selectors instead of elements when True.

        Returns:
            Selectors of relocated elements (empty when not found).

        Raises:
            FetcherError: If relocation fails.
            ValueError: If percentage is out of range.
        """
        if not 0 <= percentage <= 100:
            raise ValueError("percentage must be between 0 and 100")
        try:
            if isinstance(element, str):
                element = self._selector.retrieve(element)
                if element is None:
                    return Selectors([], self._url)
            target = element._selector if isinstance(element, Selector) else element
            if selector_type:
                result = self._selector.relocate(target, percentage, True)
            else:
                result = self._selector.relocate(target, percentage, False)
            return Selectors(result, self._url)
        except Exception as e:
            raise FetcherError("adaptive relocate failed", url=self._url) from e

    @property
    def text(self) -> str:
        """Return the text content of the element.

        Returns:
            The element text.

        Raises:
            FetcherError: If reading the text fails.
        """
        try:
            return str(self._selector.text)
        except Exception as e:
            raise FetcherError("text property failed", url=self._url) from e

    @property
    def attrib(self) -> dict[str, Any]:
        """Return the attributes of the element.

        Returns:
            A mapping of attribute name to value.

        Raises:
            FetcherError: If reading attributes fails.
        """
        try:
            return dict(self._selector.attrib)
        except Exception as e:
            raise FetcherError("attrib property failed", url=self._url) from e

    @property
    def html_content(self) -> str:
        """Return the inner HTML of the element.

        Returns:
            The element's HTML as a string.

        Raises:
            FetcherError: If reading the HTML fails.
        """
        try:
            return str(self._selector.html_content)
        except Exception as e:
            raise FetcherError("html_content property failed", url=self._url) from e

    @classmethod
    def _wrap_single(cls, selector: Any, url: str) -> "Selector":
        """Return a Selector for a single scrapling selector without re-parsing.

        Args:
            selector: A scrapling Selector instance.
            url: Base URL to attach to the Selector.

        Returns:
            A Selector around the given selector.
        """
        instance = object.__new__(cls)
        instance._selector = selector
        instance._url = url
        return instance

    def __repr__(self) -> str:
        """Return a string representation of the Selector."""
        return f"Selector(url={self._url!r})"


class Selectors:
    """Ordered container of matched elements returned by selector queries."""

    def __init__(self, selectors: Any, url: str) -> None:
        """Initialize the list container.

        Args:
            selectors: A scrapling Selectors instance or a plain list.
            url: Base URL shared by every element.
        """
        self._selectors: Any = selectors
        self._url = url

    def __iter__(self) -> Iterator[Selector]:
        """Iterate over wrapped elements.

        Yields:
            Selector for each element.
        """
        for sel in self._selectors:
            yield Selector._wrap_single(sel, self._url)

    def __len__(self) -> int:
        """Return the number of matched elements."""
        return len(self._selectors)

    def __getitem__(self, idx: Union[int, slice]) -> Any:
        """Return the element or slice at ``idx``.

        Args:
            idx: Integer index or slice.

        Returns:
            A Selector, or a Selectors for a slice.
        """
        res = self._selectors[idx]
        if isinstance(res, list):
            return Selectors(res, self._url)
        return Selector._wrap_single(res, self._url)

    def get(self, default: Any = None) -> Any:
        """Return the text of the first element, or ``default`` when empty.

        Mirrors scrapling's ``Selectors.get``: the result is a serialized
        string, not an element.

        Args:
            default: Value returned when there are no matches.

        Returns:
            The first element's text, or ``default``.

        Raises:
            FetcherError: If retrieval fails.
        """
        try:
            return self._selectors.get(default)
        except Exception as e:
            raise FetcherError("list get failed", url=self._url) from e

    def getall(self) -> List[str]:
        """Return the text of every matched element.

        Returns:
            A list of element texts.

        Raises:
            FetcherError: If extraction fails.
        """
        try:
            return [str(item) for item in self._selectors.getall()]
        except Exception as e:
            raise FetcherError("list getall failed", url=self._url) from e

    def first(self) -> Selector:
        """Return the first matched element as a Selector."""
        return Selector._wrap_single(self._selectors.first, self._url)

    def last(self) -> Selector:
        """Return the last matched element as a Selector."""
        return Selector._wrap_single(self._selectors.last, self._url)

    def re(self, regex: Union[str, Pattern[str]], **kwargs: Any) -> List[str]:
        """Apply a regex across all matched elements.

        Args:
            regex: Regex string or compiled pattern.
            **kwargs: Forwarded to scrapling's ``re``.

        Returns:
            A list of regex results.

        Raises:
            FetcherError: If the regex application fails.
        """
        try:
            return [str(item) for item in self._selectors.re(regex, **kwargs)]
        except Exception as e:
            raise FetcherError(f"list re failed: {regex}", url=self._url) from e

    def re_first(self, regex: Union[str, Pattern[str]], default: Any = None, **kwargs: Any) -> Any:
        """Apply a regex across all matches and return the first result.

        Args:
            regex: Regex string or compiled pattern.
            default: Value returned when nothing matches.
            **kwargs: Forwarded to scrapling's ``re_first``.

        Returns:
            The first regex result, or ``default``.

        Raises:
            FetcherError: If the regex application fails.
        """
        try:
            return self._selectors.re_first(regex, default, **kwargs)
        except Exception as e:
            raise FetcherError(f"list re_first failed: {regex}", url=self._url) from e

    def extract(self) -> List[str]:
        """Return the raw HTML of all matched elements.

        Returns:
            A list of HTML strings.

        Raises:
            FetcherError: If extraction fails.
        """
        try:
            return [str(item) for item in self._selectors.extract()]
        except Exception as e:
            raise FetcherError("list extract failed", url=self._url) from e

    def extract_first(self) -> str:
        """Return the raw HTML of the first matched element.

        Returns:
            An HTML string.

        Raises:
            FetcherError: If extraction fails.
        """
        try:
            return str(self._selectors.extract_first())
        except Exception as e:
            raise FetcherError("list extract_first failed", url=self._url) from e

    def css(self, selector: str) -> "Selectors":
        """Select descendants of the matched elements with a CSS selector.

        Args:
            selector: CSS selector expression.

        Returns:
            Selectors of matched elements.

        Raises:
            FetcherError: If selection fails.
        """
        try:
            return Selectors(self._selectors.css(selector), self._url)
        except Exception as e:
            raise FetcherError(f"list css failed: {selector}", url=self._url) from e

    def xpath(self, selector: str) -> "Selectors":
        """Select descendants of the matched elements with XPath.

        Args:
            selector: XPath selector expression.

        Returns:
            Selectors of matched elements.

        Raises:
            FetcherError: If selection fails.
        """
        try:
            return Selectors(self._selectors.xpath(selector), self._url)
        except Exception as e:
            raise FetcherError(f"list xpath failed: {selector}", url=self._url) from e

    def filter(self, predicate: Callable[[Selector], bool]) -> "Selectors":
        """Filter elements by a predicate function.

        Args:
            predicate: Function that takes a Selector and returns bool.

        Returns:
            Selectors of filtered elements.

        Raises:
            FetcherError: If filtering fails.
        """
        try:
            return Selectors(self._selectors.filter(predicate), self._url)
        except Exception as e:
            raise FetcherError("list filter failed", url=self._url) from e

    @property
    def text(self) -> str:
        """Return the text content shared by the matched elements.

        Returns:
            The combined element text.

        Raises:
            FetcherError: If reading the text fails.
        """
        try:
            return str(self._selectors.text)
        except Exception as e:
            raise FetcherError("list text property failed", url=self._url) from e

    @property
    def attrib(self) -> dict[str, Any]:
        """Return the attributes of the first matched element.

        Returns:
            A mapping of attribute name to value.

        Raises:
            FetcherError: If reading attributes fails.
        """
        try:
            return dict(self._selectors.attrib)
        except Exception as e:
            raise FetcherError("list attrib property failed", url=self._url) from e

    @property
    def html_content(self) -> str:
        """Return the inner HTML of the first matched element.

        Returns:
            The element's HTML as a string.

        Raises:
            FetcherError: If reading the HTML fails.
        """
        try:
            return str(self._selectors.html_content)
        except Exception as e:
            raise FetcherError("list html_content property failed", url=self._url) from e

    def __repr__(self) -> str:
        """Return a string representation of the Selectors."""
        return f"Selectors(url={self._url!r}, count={len(self._selectors)})"
