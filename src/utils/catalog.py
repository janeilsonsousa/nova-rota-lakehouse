# Registra as tabelas Delta (gravadas por path) no Unity Catalog, só em env=databricks.
# LOCATION não funciona quando o path é um Volume, então tenta LOCATION e cai pra CTAS.

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from src.config.settings import PipelineConfig


def register_delta_table(spark: SparkSession, config: PipelineConfig, layer: str, table: str) -> None:
    if config.env != "databricks":
        return
    path = config.table_path(layer, table)
    if not DeltaTable.isDeltaTable(spark, path):
        return
    fqn = config.table_fqn(layer, table)
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {fqn} USING DELTA LOCATION '{path}'")
        return
    except Exception:
        pass  # Volume não aceita LOCATION, tenta CTAS
    try:
        spark.sql(f"CREATE OR REPLACE TABLE {fqn} AS SELECT * FROM delta.`{path}`")
    except Exception:
        pass  # best-effort, o pipeline não depende disso pra funcionar


def register_known_tables(spark: SparkSession, config: PipelineConfig, layer: str, tables: list[str]) -> None:
    for table in tables:
        register_delta_table(spark, config, layer, table)
