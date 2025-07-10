import logging

from sentry_sdk import start_span

from utils.logging_configuration import (
    CustomDatadogJsonFormatter,
    CustomLocalJsonFormatter,
)


class TestLoggingConfig:
    def test_local_formatter(self):
        log_record = {"levelname": "weird_level", "message": "This is a message"}
        cljf = CustomLocalJsonFormatter()
        res = cljf.jsonify_log_record(log_record)
        assert "weird_level: This is a message \n {}" == res

    def test_local_formatter_with_exc_info(self):
        log_record = {
            "levelname": "weird_level",
            "message": "This is a message",
            "exc_info": "Line\nWith\nbreaks",
        }
        cljf = CustomLocalJsonFormatter()
        res = cljf.jsonify_log_record(log_record)
        assert "weird_level: This is a message \n {}\nLine\nWith\nbreaks" == res

    def test_datadog_formatter_with_trace_id(self):
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="This is a message",
            args=(),
            exc_info=None,
        )
        record.levelname = "weird_level"

        log_record = {
            "levelname": "weird_level",
            "message": "This is a message",
            "asctime": "2023-10-01 12:00:00,000",
        }

        with start_span(op="test_logging_config", trace_id="12345"):
            cdjf = CustomDatadogJsonFormatter()
            cdjf.add_fields(
                log_record,
                record,
                {},
            )

            # Check that the trace_id was added to the log_record
            assert "sentry_trace_id" in log_record
            assert log_record["sentry_trace_id"] == "12345"
            assert log_record["level"] == "weird_level"
