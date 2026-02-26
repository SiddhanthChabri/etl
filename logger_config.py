"""
logger_config.py — Structured JSON Logging

Outputs JSON-structured log lines (parseable by Grafana Loki, Datadog, etc.)
while keeping a human-readable console formatter for local development.

Usage:
    from logger_config import setup_logger
    log = setup_logger("MyModule")
    log.info("Processing started", extra={"records": 1234})
"""

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone


# ── JSON formatter ────────────────────────────────────────────────────────────

class JsonFormatter(logging.Formatter):
    """
    Formats log records as single-line JSON objects.
    Fields: timestamp, level, logger, message, + any extra keys.
    """

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level"    : record.levelname,
            "logger"   : record.name,
            "message"  : record.getMessage(),
            "module"   : record.module,
            "func"     : record.funcName,
            "line"     : record.lineno,
        }

        # Merge any extra fields passed via extra={...}
        for key, val in record.__dict__.items():
            if key not in {
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "message",
                "taskName",
            } and not key.startswith("_"):
                payload[key] = val

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)

        return json.dumps(payload, default=str)


# ── Human-readable formatter (console only) ───────────────────────────────────

class HumanFormatter(logging.Formatter):
    COLORS = {
        "DEBUG"   : "\033[36m",
        "INFO"    : "\033[32m",
        "WARNING" : "\033[33m",
        "ERROR"   : "\033[31m",
        "CRITICAL": "\033[35m",
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        color = self.COLORS.get(record.levelname, "")
        ts    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        base  = f"{ts} {color}{record.levelname:8s}{self.RESET} [{record.name}] {record.getMessage()}"
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


# ── Public factory ────────────────────────────────────────────────────────────

def setup_logger(name: str = "ETL", log_file: str = "etl_logs.jsonl") -> logging.Logger:
    """
    Create (or retrieve) a logger that writes:
      - JSON lines to  logs/<date>_<log_file>
      - Human-readable to stdout

    Args:
        name:     Logger name (shown in every log line as 'logger').
        log_file: Base filename for the JSON log file (default: etl_logs.jsonl).

    Returns:
        logging.Logger configured with both handlers.
    """
    logger = logging.getLogger(name)

    # Idempotent — don't add handlers twice
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    date_prefix = datetime.now().strftime("%Y%m%d")

    # ── File handler: JSON lines ──────────────────────────────────────────────
    json_path = os.path.join(log_dir, f"{date_prefix}_{log_file}")
    fh = logging.FileHandler(json_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(JsonFormatter())
    logger.addHandler(fh)

    # ── Console handler: human-readable ──────────────────────────────────────
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(HumanFormatter())

    # Windows UTF-8 fix
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        except Exception:
            pass

    logger.addHandler(ch)

    return logger


# ── Optional: application-level structured event helpers ─────────────────────

def log_event(logger: logging.Logger, event: str, **kwargs) -> None:
    """
    Log a structured business event.

    Example:
        log_event(log, "etl.load.complete", records=5000, table="fact_sales")
    """
    logger.info(event, extra={"event": event, **kwargs})
