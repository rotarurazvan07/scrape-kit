# Parsing

The `scrape_kit.parse` package provides a thin facade over scrapling's parser, exposing CSS/XPath selection, text search, adaptive element relocation, and link extraction behind scrape-kit's exception boundary.

All exceptions are converted to `FetcherError` with the source URL chained as `__cause__`. The underlying scrapling objects are never exposed directly.

## Quick start

```python
from scrape_kit import parse

selector = parse(html, url="https://example.com")
prices = selector.css(".price").getall()
```

## Selector

Returned by `parse()`. Mirrors scrapling's `Selector` API.

```python
from scrape_kit import parse

selector = parse(html, url="https://example.com")

# CSS selection
prices = selector.css(".price").getall()

# XPath selection
links = selector.xpath("//a/@href")

# Text-based search
element = selector.find_by_text("Add to cart")

# Similar element search
similar = selector.find_similar(similarity_threshold=0.3)

# Regex search
match = selector.find_by_regex(r"price: \d+")

# Extraction
first_price = selector.css(".price").get()
all_prices = selector.css(".price").getall()

# Properties
text = selector.text          # element text
attrs = selector.attrib       # element attributes
html = selector.html_content  # inner HTML
url = selector.url            # base URL

# Adaptive relocation
selector.save(element, "my_id")
relocated = selector.relocate("my_id")

# Markdown (requires scrapling[rag] extra)
# markdown = selector.markdown()
```

## Selectors

Returned by selection methods (`css()`, `xpath()`, etc.). Provides list-like access plus first/last/get/getall helpers.

```python
from scrape_kit import parse

items = parse(html).css(".product")

# Iteration
for item in items:
    print(item.text)

# Indexing
first = items[0]

# Slicing returns a Selectors
subset = items[0:2]

# Helpers
first = items.first()
last = items.last()
all_text = items.getall()
```

## Adaptive relocation

Adaptive relocation survives site redesigns by storing element fingerprints and finding the corresponding element on a new page version.

```python
from scrape_kit import parse

# First page version - save element fingerprint
selector = parse(html_v1, url="https://shop.example/p/1", adaptive=True)
price_element = selector.css(".price")[0]
selector.save(price_element, "price")

# Later - same URL, new HTML, find the relocated element
relocated = parse.adaptive_select(
    html_v2, url="https://shop.example/p/1", selector=".price",
    auto_save=True, adaptive_domain="shop.example"
)
```

Lower-level helper:

```python
from scrape_kit import parse

# Relocate on new HTML, given a previously saved identifier
relocated = parse.relocate(new_html, "https://example.com", "my_id")
```

### Storage isolation

The `adaptive_domain` parameter isolates adaptive storage between different sites. By default it uses the document's URL.

```python
# Separate storage for different domains
parse.adaptive_select(html, url, ".price", adaptive_domain="shop.example")
parse.adaptive_select(html, url, ".cost", adaptive_domain="market.example")
```

## Link extraction

```python
from scrape_kit import parse, fetch

html = fetch("https://example.com")

# Simple link extraction
urls = parse.follow_links(html, "a.product", base_url="https://example.com")

# With metadata
links = parse.extract_links(html, "a.product", base_url="https://example.com")
for link in links:
    print(link["url"], link["text"], link["attributes"])
```

Works with scrapling `Response` objects too:

```python
from scrape_kit import browser, parse

with browser() as session:
    response = session.fetch("https://example.com")
    urls = parse.follow_links(response, "a.product")
```

### CSS vs XPath dispatch

By default the selector type is auto-detected: a selector starting with `//` (or `/` not followed by `*`) is treated as XPath, anything else as CSS. Pass `selector_type` explicitly to remove the guesswork.

```python
from scrape_kit import parse

parse.follow_links(html, "a.product", base_url=url, selector_type="css")
parse.follow_links(html, "//a[@class='product']", base_url=url, selector_type="xpath")
```

## Error handling

All parse operations raise `FetcherError` (or the `ParseError` alias) with the source URL available on `exc.url`. Invalid arguments (empty selector, out-of-range percentage, empty url/identifier) raise `ValueError` before any parsing work happens.

```python
from scrape_kit import parse, FetcherError

try:
    selector = parse(html, url="https://example.com")
except FetcherError as exc:
    print(exc.url, exc)
```

## Limitations

- `Selector.markdown()` is exposed but requires the `scrapling[rag]` extra (not a dependency); it raises `NotImplementedError` with install guidance when the extra is absent.
- `Response.follow()` with callback is not wrapped; use `extract_links()` + `fetch()` loop instead.

## See also

- [Fetching](fetcher.md) - for `fetch()`, `browser()`, `scrape()`
- [Errors](errors.md) - exception hierarchy
