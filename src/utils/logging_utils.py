"""Logging estruturado (JSON por linha) para o pipeline.

Cada etapa do pipeline (bronze/silver/gold) deve logar início, fim,
contagem de linhas processadas/rejeitadas e erros usando este logger, para
que em produção (Databricks Workflows) as mensagens sejam facilmente
correlacionáveis por ``batch_id`` em uma ferramenta de observabilidade
(ex.: exportadas para uma tabela de log ou para o Log Analytics workspace).
"""

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


def get_logger(name: str, batch_id: str | None = None) -> "_ContextLogger":
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return _ContextLogger(logger, {"batch_id": batch_id} if batch_id else {})


class _ContextLogger:
    """Wrapper fino que injeta campos de contexto (ex.: batch_id, etapa)."""

    def __init__(self, logger: logging.Logger, context: dict[str, Any]):
        self._logger = logger
        self._context = context

    def bind(self, **kwargs: Any) -> "_ContextLogger":
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
