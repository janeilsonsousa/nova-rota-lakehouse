"""Registro de tabelas Delta (escritas por path) no catálogo SQL.

O pipeline escreve/lê todas as tabelas por **path físico**
(`config.table_path(...)`), não por nome de catálogo — isso é o que torna o
mesmo código 100% portátil entre execução local (sem metastore) e
Databricks. Mas para que `sql/advanced_queries.sql` e os notebooks de
análise consigam fazer `SELECT ... FROM nova_rota.gold.gold_fato_transacao`,
a tabela física precisa estar registrada no catálogo (Unity Catalog ou
`hive_metastore`/`spark_catalog` default).

Esta função só roda em ``env == "databricks"`` (em execução local não há
necessidade de catálogo — os testes/scripts locais leem direto por path) e é
idempotente (`CREATE TABLE IF NOT EXISTS ... LOCATION`).
"""

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
    except Exception:
        # Best-effort: um Volume Unity Catalog é válido para arquivos
        # (dado bruto, checkpoints, quarentena) mas NÃO é aceito como
        # LOCATION de tabela registrada no catálogo (Unity Catalog exige
        # um External Location com storage credential para isso, ou uma
        # tabela totalmente managed sem LOCATION explícito). O pipeline
        # já lê/escreve por path físico e não depende deste registro para
        # funcionar — ele só existe para conveniência de consulta SQL por
        # nome. Ver docs/decisions.md.
        pass


def register_known_tables(spark: SparkSession, config: PipelineConfig, layer: str, tables: list[str]) -> None:
    for table in tables:
        register_delta_table(spark, config, layer, table)
