# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestão Bronze
# MAGIC
# MAGIC Executa a ingestão incremental das 6 fontes (clientes, contas, cartões,
# MAGIC transações, eventos de risco, estornos) a partir de
# MAGIC `data/raw/nova_rota_input/` (ou de um Volume Unity Catalog, se
# MAGIC `raw_path` apontar para lá).
# MAGIC
# MAGIC Este notebook é a camada de **execução/demonstração** — toda a lógica
# MAGIC vive em `src/ingestion/`, testada em `tests/transform/test_bronze_ingestion.py`.

# COMMAND ----------

import sys
from pathlib import Path

repo_root = Path.cwd()
while not (repo_root / "src").exists() and repo_root != repo_root.parent:
    repo_root = repo_root.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# COMMAND ----------

dbutils.widgets.text("catalog", "nova_rota")
dbutils.widgets.text("run_mode", "incremental")
dbutils.widgets.text("raw_path", str(repo_root / "data" / "raw" / "nova_rota_input"))
dbutils.widgets.text("base_path", "/Volumes/nova_rota/bronze/storage/lakehouse")
dbutils.widgets.text("checkpoint_path", "/Volumes/nova_rota/bronze/storage/checkpoints")
dbutils.widgets.text("quarantine_path", "/Volumes/nova_rota/bronze/storage/lakehouse/_quarentena")

# COMMAND ----------

from src.config import get_config  # noqa: E402
from src.ingestion.bronze import run_bronze_ingestion  # noqa: E402

overrides = {
    "env": "databricks",
    "catalog": dbutils.widgets.get("catalog"),
    "run_mode": dbutils.widgets.get("run_mode"),
    "raw_path": dbutils.widgets.get("raw_path"),
}
if dbutils.widgets.get("base_path"):
    overrides["base_path"] = dbutils.widgets.get("base_path")
if dbutils.widgets.get("checkpoint_path"):
    overrides["checkpoint_path"] = dbutils.widgets.get("checkpoint_path")
if dbutils.widgets.get("quarantine_path"):
    overrides["quarantine_path"] = dbutils.widgets.get("quarantine_path")

config = get_config(**overrides)
print("Configuração desta execução:", config.as_dict())

# COMMAND ----------

resultados = run_bronze_ingestion(spark, config)
display(spark.createDataFrame(resultados))

# COMMAND ----------

# MAGIC %md ## Evidência: tabelas Bronze criadas + amostra

# COMMAND ----------

for tabela in ["bronze_clientes_cdc", "bronze_contas_cdc", "bronze_cartoes_cdc", "bronze_transacoes", "bronze_eventos_risco", "bronze_estornos"]:
    df = spark.read.format("delta").load(config.table_path("bronze", tabela))
    print(f"{tabela}: {df.count()} linhas, {len(df.columns)} colunas")

# COMMAND ----------

display(spark.read.format("delta").load(config.table_path("bronze", "bronze_transacoes")).limit(10))

# COMMAND ----------

# MAGIC %md ## Evidência: idempotência (reexecutar não duplica)

# COMMAND ----------

resultados_reexecucao = run_bronze_ingestion(spark, config)
display(spark.createDataFrame(resultados_reexecucao))
assert all(r["linhas_ingeridas"] == 0 for r in resultados_reexecucao), "reexecução deveria processar 0 linhas novas"
print("OK — pipeline idempotente confirmado.")
