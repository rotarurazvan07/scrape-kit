import logging
import os
import sys
from unittest.mock import patch

import pytest

from scrape_kit.logger import ScrapeKitFormatter, get_logger, time_profiler

pytestmark = pytest.mark.p0


@pytest.fixture(autouse=True)
def restore_logger_state():
    """Snapshot/restore the process-global 'test_logger' logger around every test (#16)."""
    target = logging.getLogger("test_logger")
    level, handlers, propagate = target.level, list(target.handlers), target.propagate
    yield
    target.setLevel(level)
    for h in list(target.handlers):
        target.removeHandler(h)
        if h not in handlers:
            h.close()
    for h in handlers:
        target.addHandler(h)
    target.propagate = propagate


class TestScrapeKitFormatter:
    """ScrapeKitFormatter renders level, name and message for every log level."""

    @pytest.mark.parametrize(
        ("level", "level_name", "message"),
        [
            (logging.DEBUG, "DEBUG", "Debug message"),
            (logging.INFO, "INFO", "Info message"),
            (logging.WARNING, "WARNING", "Warning message"),
            (logging.ERROR, "ERROR", "Error message"),
            (logging.CRITICAL, "CRITICAL", "Critical message"),
        ],
    )
    def test_normal_formats_message(self, level, level_name, message):
        """Every log level renders level, logger name and message."""
        record = logging.LogRecord(
            name="test",
            level=level,
            pathname="test.py",
            lineno=1,
            msg=message,
            args=(),
            exc_info=None,
        )
        formatted = ScrapeKitFormatter().format(record)
        assert level_name in formatted
        assert "test" in formatted
        assert message in formatted


class TestGetLogger:
    """get_logger configures level, handlers, propagation and stream targets."""

    @pytest.mark.smoke
    def test_normal_creates_logger_with_default_info_level(self):
        logger = get_logger("test_logger")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_logger"
        assert logger.level == logging.INFO
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)

    def test_normal_sets_custom_level(self):
        logger = get_logger("test_logger", level=logging.INFO)
        assert logger.level == logging.INFO

    def test_normal_uses_environment_variable_for_level(self):
        with patch.dict(os.environ, {"SCRAPE_KIT_LOG_LEVEL": "WARNING"}):
            logger = get_logger("test_logger")
            assert logger.level == logging.WARNING

    def test_normal_uses_environment_variable_invalid_defaults_to_info(self):
        with patch.dict(os.environ, {"SCRAPE_KIT_LOG_LEVEL": "INVALID"}):
            logger = get_logger("test_logger")
            assert logger.level == logging.INFO

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
    """time_profiler decorates with and without parens, times and re-raises."""

    def test_normal_decorator_measures_execution_time(self):
        @time_profiler()
        def sample_function():
            return "result"

        result = sample_function()
        assert result == "result"

    def test_normal_decorator_without_parens(self):
        @time_profiler
        def sample_function():
            return "result"

        result = sample_function()
        assert result == "result"

    def test_normal_custom_logging_level(self):
        @time_profiler(level=logging.WARNING)
        def sample_function():
            return "result"

        result = sample_function()
        assert result == "result"

    def test_normal_function_with_arguments(self):
        @time_profiler()
        def sample_function(arg1, arg2, kwarg1=None):
            return f"{arg1}_{arg2}_{kwarg1}"

        result = sample_function("hello", "world", kwarg1="test")
        assert result == "hello_world_test"

    def test_normal_exception_function_still_times_and_raises(self):
        @time_profiler()
        def sample_function():
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            sample_function()

    def test_normal_logger_with_no_handlers_gets_configured(self):
        # Create a unique module name for testing
        test_logger = logging.getLogger("unique_test_module_logger")
        test_logger.handlers.clear()

        # Patch the logging module's getLogger to return our test logger
        with patch("logging.getLogger", return_value=test_logger):

            @time_profiler()
            def sample_function():
                return "result"

            result = sample_function()
            assert result == "result"
