"""Central logging: get_logger factory, colourised formatter, and the time_profiler decorator."""

import logging
import os
import sys
import time
from collections.abc import Callable
from functools import wraps
from typing import Any, overload


class ScrapeKitFormatter(logging.Formatter):
    """
    Custom formatter with colors for terminal output.
    """

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    blue = "\x1b[34;20m"
    reset = "\x1b[0m"
    format_str = "%(asctime)s - %(name)s - %(levelname)8s - %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format_str + reset,
        logging.INFO: blue + format_str + reset,
        logging.WARNING: yellow + format_str + reset,
        logging.ERROR: red + format_str + reset,
        logging.CRITICAL: bold_red + format_str + reset,
    }

    def format(self, record: logging.LogRecord) -> str:
        """Format a record using the colourised scheme for its level."""
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


# ponytail: flat kwargs beat a config object — every knob is used independently by callers;
# introduce a profile/bundle type only if a real caller needs to pass them as a group.


def get_logger(
    name: str,
    level: int | None = None,
    log_file: str | None = None,
    stream: Any = sys.stderr,
    propagate: bool = False,
) -> logging.Logger:
    """
    Get a configured logger with optional file/stream output and custom level.

    Args:
        name: Name of the logger (usually __name__)
        level: Logging level (e.g. logging.DEBUG). Defaults to SCRAPE_KIT_LOG_LEVEL env var or INFO.
        log_file: Optional path to a file to write logs to.
        stream: Stream to output logs to (e.g. sys.stdout, sys.stderr). Defaults to sys.stderr.
        propagate: Whether to propagate logs to parent loggers.
    """
    logger = logging.getLogger(name)

    # Set level from argument, environment variable, or default to INFO
    if level is None:
        env_level = os.environ.get("SCRAPE_KIT_LOG_LEVEL", "INFO").upper()
        level = getattr(logging, env_level, logging.INFO)

    logger.setLevel(level)
    logger.propagate = propagate

    # Clear existing handlers to avoid duplicates if re-initialized
    if logger.handlers:
        logger.handlers.clear()

    # Console/Stream Handler
    console_handler = logging.StreamHandler(stream)
    console_handler.setFormatter(ScrapeKitFormatter())
    logger.addHandler(console_handler)

    # File Handler (if requested)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


_MS_PER_SECOND = 1000


def _timed_call(func: Callable[..., Any], level: int, *args: Any, **kwargs: Any) -> Any:
    """Call func, then log its elapsed duration in milliseconds at level."""
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    duration_ms = (time.perf_counter() - start_time) * _MS_PER_SECOND

    logger = logging.getLogger(func.__module__)
    # If the logger isn't configured yet, get_logger will handle it
    if not logger.handlers:
        logger = get_logger(func.__module__)

    logger.log(level, "Function '%s' took %.2fms", func.__name__, duration_ms)
    return result


@overload
def time_profiler(
    level: int = ...,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Factory form: ``@time_profiler(level)`` returns a decorator for the wrapped function."""


@overload
def time_profiler(level: Callable[..., Any]) -> Callable[..., Any]:
    """Bare-decorator form: ``@time_profiler`` returns the wrapped function directly."""


def time_profiler(
    level: int | Callable[..., Any] = logging.DEBUG,
) -> Callable[..., Any]:
    """
    Decorator to log execution time of a function.

    Usable as ``@time_profiler(level)`` (factory form) or as a bare
    ``@time_profiler`` decorator; in the bare form the duration is
    logged at ``logging.DEBUG``.

    Args:
        level: The logging level for the duration message. May also be the
            decorated function itself when ``@time_profiler`` is used
            without parentheses.

    Returns:
        The decorator (when called with a level) or the wrapped function
        (when used as a bare decorator).
    """

    log_level: int = logging.DEBUG if callable(level) else level

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        """Wrap func so each call logs its duration at the configured level."""

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """Time a single invocation of the wrapped function."""
            return _timed_call(func, log_level, *args, **kwargs)

        return wrapper

    # Support @time_profiler without parens
    if callable(level):
        return decorator(level)
    return decorator
