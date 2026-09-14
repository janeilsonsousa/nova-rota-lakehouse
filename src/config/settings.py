# Config central do pipeline. Dá pra sobrescrever tudo via env var NOVAROTA_* ou kwargs em get_config().

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.environ.get(f"NOVAROTA_{name}", default)


def _project_root() -> Path:
    # src/config/settings.py -> sobe 2 níveis até a raiz do repo
    return Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class PipelineConfig:
    # env: local (Spark local + Delta OSS) ou databricks (Unity Catalog)
    env: str = "local"
    catalog: str = "nova_rota"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    base_path: str = ""
    raw_path: str = ""
    checkpoint_path: str = ""
    quarantine_path: str = ""

    data_referencia: date = field(default_factory=date.today)
    run_mode: str = "incremental"
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __post_init__(self) -> None:
        if self.run_mode not in {"incremental", "full"}:
            raise ValueError(f"run_mode inválido: {self.run_mode!r} (use 'incremental' ou 'full')")
        if self.env not in {"local", "databricks"}:
            raise ValueError(f"env inválido: {self.env!r} (use 'local' ou 'databricks')")

    def table_fqn(self, layer: str, table: str) -> str:
        # catalogo.schema.tabela no Databricks, schema.tabela local
        schema = self._schema_for(layer)
        if self.env == "databricks":
            return f"{self.catalog}.{schema}.{table}"
        return f"{schema}.{table}"

    def table_path(self, layer: str, table: str) -> str:
        schema = self._schema_for(layer)
        return f"{self.base_path}/{schema}/{table}"

    def _schema_for(self, layer: str) -> str:
        mapping = {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
        }
        if layer not in mapping:
            raise ValueError(f"camada inválida: {layer!r} (use bronze/silver/gold)")
        return mapping[layer]

    @property
    def quarantine_table_path(self) -> str:
        return self.quarantine_path

    def as_dict(self) -> dict:
        d = {
            "env": self.env,
            "catalog": self.catalog,
            "bronze_schema": self.bronze_schema,
            "silver_schema": self.silver_schema,
            "gold_schema": self.gold_schema,
            "base_path": self.base_path,
            "raw_path": self.raw_path,
            "checkpoint_path": self.checkpoint_path,
            "quarantine_path": self.quarantine_path,
            "data_referencia": self.data_referencia.isoformat(),
            "run_mode": self.run_mode,
            "batch_id": self.batch_id,
        }
        return d


def get_config(**overrides) -> PipelineConfig:
    # prioridade: kwargs > env var NOVAROTA_* > default
    root = _project_root()
    env = overrides.pop("env", _env("ENV", "local"))

    default_base_path = str(root / "data" / "lakehouse")
    default_raw_path = str(root / "data" / "raw" / "nova_rota_input")
    default_checkpoint_path = str(root / "data" / "checkpoints")
    default_quarantine_path = str(root / "data" / "lakehouse" / "_quarentena")

    base_path = overrides.pop("base_path", _env("BASE_PATH", default_base_path))
    raw_path = overrides.pop("raw_path", _env("RAW_PATH", default_raw_path))
    checkpoint_path = overrides.pop("checkpoint_path", _env("CHECKPOINT_PATH", default_checkpoint_path))
    quarantine_path = overrides.pop("quarantine_path", _env("QUARANTINE_PATH", default_quarantine_path))

    catalog = overrides.pop("catalog", _env("CATALOG", "nova_rota"))
    bronze_schema = overrides.pop("bronze_schema", _env("BRONZE_SCHEMA", "bronze"))
    silver_schema = overrides.pop("silver_schema", _env("SILVER_SCHEMA", "silver"))
    gold_schema = overrides.pop("gold_schema", _env("GOLD_SCHEMA", "gold"))

    run_mode = overrides.pop("run_mode", _env("RUN_MODE", "incremental"))
    batch_id = overrides.pop("batch_id", _env("BATCH_ID", "") or str(uuid.uuid4()))

    data_ref_raw = overrides.pop("data_referencia", _env("DATA_REFERENCIA", ""))
    if isinstance(data_ref_raw, date):
        data_referencia = data_ref_raw
    elif data_ref_raw:
        data_referencia = datetime.strptime(data_ref_raw, "%Y-%m-%d").date()
    else:
        data_referencia = date.today()

    return PipelineConfig(
        env=env,
        catalog=catalog,
        bronze_schema=bronze_schema,
        silver_schema=silver_schema,
        gold_schema=gold_schema,
        base_path=base_path,
        raw_path=raw_path,
        checkpoint_path=checkpoint_path,
        quarantine_path=quarantine_path,
        data_referencia=data_referencia,
        run_mode=run_mode,
        batch_id=batch_id,
        **overrides,
    )
