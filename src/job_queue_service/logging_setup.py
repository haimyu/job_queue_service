"""Structured JSON logging to stdout."""

import json
import logging
import sys
from datetime import UTC, datetime

# Optional fields that job-related log lines pass through `extra={...}`.
CONTEXT_FIELDS = ("job_id", "job_type", "attempt", "worker_id")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in CONTEXT_FIELDS:
            if hasattr(record, field):
                entry[field] = getattr(record, field)
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        # default=str makes UUIDs and other non-JSON values print as strings.
        return json.dumps(entry, default=str)


def setup_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    # Replace handlers instead of adding, so calling this twice does not duplicate lines.
    root.handlers = [handler]
    root.setLevel(level)
