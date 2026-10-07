# Logging

Use `get_logger` for a colourised console logger, and `time_profiler` to time a function.

```python
from scrape_kit import get_logger

log = get_logger(__name__)
log.info("ready")
```

## get_logger

```python
import logging
import sys

from scrape_kit import get_logger

log = get_logger(
    __name__,
    level=logging.INFO,
    log_file=None,
    stream=sys.stderr,
    propagate=False,
)
```

- `name` is the logger name. Pass `__name__`.
- `level` defaults to the `SCRAPE_KIT_LOG_LEVEL` environment variable, or `INFO` when that variable is unset or not a standard level name.
- `log_file` adds a plain-text file handler when you pass a path.
- `stream` defaults to `sys.stderr`.
- `propagate` defaults to `False`.

Calling `get_logger` again on the same name replaces existing handlers so you do not stack duplicates.

```bash
export SCRAPE_KIT_LOG_LEVEL=DEBUG
```

## time_profiler

Use it as a bare decorator or with an explicit level.

```python
import logging

from scrape_kit import time_profiler

@time_profiler
def parse_fast(html: str) -> int:
    return len(html)

@time_profiler(logging.INFO)
def parse_visible(html: str) -> int:
    return len(html)
```

The bare form logs duration at `DEBUG`. The timed logger is the wrapped function's module logger.

## Related guides

- [Fetching](fetcher.md) if you want to see retry and escalation messages.
