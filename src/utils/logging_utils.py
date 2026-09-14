# Log estruturado em JSON, uma linha por evento, pra dar pra filtrar por batch_id depois.

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "extra_fields", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def get_logger(name: str, batch_id: str | None = None) -> _ContextLogger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return _ContextLogger(logger, {"batch_id": batch_id} if batch_id else {})


class _ContextLogger:
    def __init__(self, logger: logging.Logger, context: dict[str, Any]):
        self._logger = logger
        self._context = context

    def bind(self, **kwargs: Any) -> _ContextLogger:
        return _ContextLogger(self._logger, {**self._context, **kwargs})

    def _log(self, level: int, message: str, **kwargs: Any) -> None:
        fields = {**self._context, **kwargs}
        self._logger.log(level, message, extra={"extra_fields": fields})

    def info(self, message: str, **kwargs: Any) -> None:
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        self._log(logging.ERROR, message, **kwargs)

    def exception(self, message: str, **kwargs: Any) -> None:
        fields = {**self._context, **kwargs}
        self._logger.exception(message, extra={"extra_fields": fields})
