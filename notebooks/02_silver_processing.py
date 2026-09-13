# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Processamento Prata
# MAGIC
# MAGIC Qualidade de dados + quarentena, SCD2 (clientes/contas/cartões) e MERGE
# MAGIC incremental (transações/eventos/estornos). Lógica em `src/silver/`,
# MAGIC testada em `tests/transform/test_silver_*.py`.
# MAGIC
# MAGIC **Pré-requisito**: rodar `01_bronze_ingestion` antes deste notebook.

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
dbutils.widgets.text("run_mode", "full")  # cada notebook gera seu próprio batch_id; full evita "nada a fazer" entre execuções separadas
dbutils.widgets.text("base_path", "/Volumes/nova_rota/bronze/storage/lakehouse")
dbutils.widgets.text("checkpoint_path", "/Volumes/nova_rota/bronze/storage/checkpoints")
dbutils.widgets.text("quarantine_path", "/Volumes/nova_rota/bronze/storage/lakehouse/_quarentena")

# COMMAND ----------

from src.config import get_config  # noqa: E402
from src.silver.run_silver import run_silver_processing  # noqa: E402

overrides = {"env": "databricks", "catalog": dbutils.widgets.get("catalog"), "run_mode": dbutils.widgets.get("run_mode")}
if dbutils.widgets.get("base_path"):
    overrides["base_path"] = dbutils.widgets.get("base_path")
if dbutils.widgets.get("checkpoint_path"):
    overrides["checkpoint_path"] = dbutils.widgets.get("checkpoint_path")
if dbutils.widgets.get("quarantine_path"):
    overrides["quarantine_path"] = dbutils.widgets.get("quarantine_path")
config = get_config(**overrides)
print(config.as_dict())

# COMMAND ----------

resultados = run_silver_processing(spark, config)
display(spark.createDataFrame(resultados))

# COMMAND ----------

# MAGIC %md ## Evidência: histórico SCD2 (cliente com múltiplas versões)

# COMMAND ----------

silver_clientes = spark.read.format("delta").load(config.table_path("silver", "silver_clientes"))
clientes_com_historico = (
    silver_clientes.groupBy("id_cliente").count().filter("count > 1").select("id_cliente").limit(3)
)
display(
    silver_clientes.join(clientes_com_historico, "id_cliente")
    .select("id_cliente", "cidade", "renda", "segmento", "versao", "dt_inicio_vigencia", "dt_fim_vigencia", "flag_vigente")
    .orderBy("id_cliente", "versao")
)

# COMMAND ----------

# MAGIC %md ## Evidência: quarentena por entidade e motivo

# COMMAND ----------

for entidade in ["clientes", "contas", "cartoes", "transacoes", "eventos_risco", "estornos"]:
    path = f"{config.quarantine_path}/{entidade}"
    try:
        df = spark.read.format("delta").load(path)
        print(f"\n--- quarentena {entidade}: {df.count()} linhas ---")
        df.groupBy("motivos_quarentena").count().show(truncate=False)
    except Exception:
        print(f"\n--- quarentena {entidade}: nenhum registro reprovado ---")

# COMMAND ----------

# MAGIC %md ## Evidência: MERGE idempotente (transações não duplicam entre lotes)

# COMMAND ----------

silver_transacoes = spark.read.format("delta").load(config.table_path("silver", "silver_transacoes"))
duplicadas = silver_transacoes.groupBy("id_transacao").count().filter("count > 1")
print("Transações com id_transacao duplicado na Prata:", duplicadas.count())
assert duplicadas.count() == 0, "MERGE deveria garantir unicidade por id_transacao"
print("OK — nenhuma duplicidade encontrada.")
