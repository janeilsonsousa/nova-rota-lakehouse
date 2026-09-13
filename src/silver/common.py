"""Utilitários compartilhados pelos módulos de transformação da Prata."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config.settings import PipelineConfig


def read_bronze_batch(spark: SparkSession, config: PipelineConfig, bronze_table: str) -> DataFrame:
    """Lê a fatia de Bronze relevante para esta execução.

    ``run_mode='incremental'`` (padrão): apenas as linhas com o
    ``batch_id`` desta execução — a Prata processa exatamente o que a
    Bronze acabou de ingerir no mesmo run do pipeline.
    ``run_mode='full'``: toda a tabela Bronze, para backfill/reprocessamento
    completo (ex.: corrigir uma regra de negócio retroativamente).
    """
    path = config.table_path("bronze", bronze_table)
    if not DeltaTable.isDeltaTable(spark, path):
        raise RuntimeError(
            f"Tabela Bronze '{bronze_table}' ainda não existe em {path}. "
            "Execute a ingestão Bronze antes da Prata (run_pipeline.py garante essa ordem)."
        )
    df = spark.read.format("delta").load(path)
    if config.run_mode == "full":
        return df
    return df.filter(F.col("batch_id") == config.batch_id)


def is_empty(df: DataFrame) -> bool:
    return df.limit(1).count() == 0
