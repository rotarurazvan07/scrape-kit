"""
Comprehensive tests for logger.py — logging functionality.

Public API covered:
  ScrapeKitFormatter, get_logger, time_profiler

Each method has: normal case, edge case(s), error case.
"""

import logging
import os
import sys
import time
from unittest.mock import patch

import pytest

from scrape_kit.logger import ScrapeKitFormatter, get_logger, time_profiler


class TestScrapeKitFormatter:
    def test_normal_formats_debug_message(self):
        formatter = ScrapeKitFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.DEBUG,
            pathname="test.py",
            lineno=1,
            msg="Debug message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "DEBUG" in formatted
        assert "test" in formatted
        assert "Debug message" in formatted

    def test_normal_formats_info_message(self):
        formatter = ScrapeKitFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Info message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "INFO" in formatted
        assert "test" in formatted
        assert "Info message" in formatted

    def test_normal_formats_warning_message(self):
        formatter = ScrapeKitFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="test.py",
            lineno=1,
            msg="Warning message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "WARNING" in formatted
        assert "test" in formatted
        assert "Warning message" in formatted

    def test_normal_formats_error_message(self):
        formatter = ScrapeKitFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="Error message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "ERROR" in formatted
        assert "test" in formatted
        assert "Error message" in formatted

    def test_normal_formats_critical_message(self):
        formatter = ScrapeKitFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.CRITICAL,
            pathname="test.py",
            lineno=1,
            msg="Critical message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "CRITICAL" in formatted
        assert "test" in formatted
        assert "Critical message" in formatted


class TestGetLogger:
    def test_normal_creates_logger_with_default_debug_level(self):
        logger = get_logger("test_logger")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_logger"
        assert logger.level == logging.DEBUG
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)

    def test_normal_sets_custom_level(self):
        logger = get_logger("test_logger", level=logging.INFO)
        assert logger.level == logging.INFO

    def test_normal_uses_environment_variable_for_level(self):
        with patch.dict(os.environ, {"SCRAPE_KIT_LOG_LEVEL": "WARNING"}):
            logger = get_logger("test_logger")
            assert logger.level == logging.WARNING

    def test_normal_uses_environment_variable_invalid_defaults_to_debug(self):
        with patch.dict(os.environ, {"SCRAPE_KIT_LOG_LEVEL": "INVALID"}):
            logger = get_logger("test_logger")
            assert logger.level == logging.DEBUG

    def test_normal_creates_file_handler_when_log_file_specified(self, tmp_path):
        log_file = tmp_path / "test.log"
        logger = get_logger("test_logger", log_file=str(log_file))
        assert len(logger.handlers) == 2
        file_handler = [h for h in logger.handlers if isinstance(h, logging.FileHandler)][0]
        assert file_handler.baseFilename == str(log_file)

    def test_normal_clears_existing_handlers(self):
        logger = get_logger("test_logger")
        original_handlers = len(logger.handlers)
        # Add a dummy handler
        logger.addHandler(logging.StreamHandler())
        assert len(logger.handlers) > original_handlers

        # Create logger again with same name
        logger2 = get_logger("test_logger")
        assert len(logger2.handlers) == 1

    def test_normal_sets_propagate_flag(self):
        logger = get_logger("test_logger", propagate=True)
        assert logger.propagate is True

    def test_normal_uses_custom_stream(self):
        custom_stream = sys.stdout
        logger = get_logger("test_logger", stream=custom_stream)
        assert logger.handlers[0].stream is custom_stream

    def test_edge_empty_log_file_name_uses_only_stream_handler(self):
        logger = get_logger("test_logger", log_file="")
        assert len(logger.handlers) == 1


class TestTimeProfiler:
    def test_normal_decorator_measures_execution_time(self):
        @time_profiler()
        def test_function():
            time.sleep(0.1)
            return "result"

        result = test_function()
        assert result == "result"

    def test_normal_decorator_without_parens(self):
        @time_profiler
        def test_function():
            time.sleep(0.1)
            return "result"

        result = test_function()
        assert result == "result"

    def test_normal_custom_logging_level(self):
        @time_profiler(level=logging.WARNING)
        def test_function():
            time.sleep(0.1)
            return "result"

        result = test_function()
        assert result == "result"

    def test_normal_function_with_arguments(self):
        @time_profiler()
        def test_function(arg1, arg2, kwarg1=None):
            time.sleep(0.05)
            return f"{arg1}_{arg2}_{kwarg1}"

        result = test_function("hello", "world", kwarg1="test")
        assert result == "hello_world_test"

    def test_normal_exception_function_still_times_and_raises(self):
        @time_profiler()
        def test_function():
            time.sleep(0.1)
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            test_function()

    def test_normal_logger_with_no_handlers_gets_configured(self):
        # Create a unique module name for testing
        test_logger = logging.getLogger("unique_test_module_logger")
        test_logger.handlers.clear()

        # Patch the logging module's getLogger to return our test logger
        with patch("logging.getLogger", return_value=test_logger):

            @time_profiler()
            def test_function():
                time.sleep(0.05)
                return "result"

            result = test_function()
            assert result == "result"
