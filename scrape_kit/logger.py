import logging
import os
import sys
import time
from functools import wraps
from typing import Any, Callable, Optional


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

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


def get_logger(
    name: str,
    level: Optional[int] = None,
    log_file: Optional[str] = None,
    stream: Any = sys.stderr,
    propagate: bool = False,
) -> logging.Logger:
    """
    Get a configured logger with optional file/stream output and custom level.

    Args:
        name: Name of the logger (usually __name__)
        level: Logging level (e.g. logging.DEBUG). Defaults to SCRAPE_KIT_LOG_LEVEL env var or DEBUG.
        log_file: Optional path to a file to write logs to.
        stream: Stream to output logs to (e.g. sys.stdout, sys.stderr). Defaults to sys.stderr.
        propagate: Whether to propagate logs to parent loggers.
    """
    logger = logging.getLogger(name)

    # Set level from argument, environment variable, or default to DEBUG
    if level is None:
        env_level = os.environ.get("SCRAPE_KIT_LOG_LEVEL", "DEBUG").upper()
        level = getattr(logging, env_level, logging.DEBUG)

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


def time_profiler(level: int = logging.DEBUG) -> Callable:
    """
    Decorator to log execution time of a function.

    Args:
        level: The logging level to use for the duration message.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            duration_ms = (end_time - start_time) * 1000

            logger = logging.getLogger(func.__module__)
            # If the logger isn't configured yet, get_logger will handle it
            if not logger.handlers:
                logger = get_logger(func.__module__)

            logger.log(level, f"Function '{func.__name__}' took {duration_ms:.2f}ms")
            return result

        return wrapper

    # Support @time_profiler without parens
    if callable(level):
        f = level
        level = logging.DEBUG
        return decorator(f)

    return decorator
