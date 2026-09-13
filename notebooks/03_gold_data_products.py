# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Data products Ouro
# MAGIC
# MAGIC Fato transacional, dimensões e agregados de negócio. Lógica em
# MAGIC `src/gold/`, testada em `tests/transform/test_gold_layer.py`.
# MAGIC
# MAGIC **Pré-requisito**: rodar `02_silver_processing` antes deste notebook.

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

from pyspark.sql import functions as F  # noqa: E402

from src.config import get_config  # noqa: E402
from src.gold.run_gold import run_gold_processing  # noqa: E402

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

resultados = run_gold_processing(spark, config)
print(resultados)

# COMMAND ----------

# MAGIC %md ## Evidência: gold_fato_transacao — atributos ponto-no-tempo

# COMMAND ----------

display(
    spark.read.format("delta").load(config.table_path("gold", "gold_fato_transacao"))
    .select(
        "id_transacao", "dt_transacao", "id_cliente_na_data", "cidade_cliente_na_data",
        "segmento_cliente_na_data", "valor", "valor_liquido", "flag_estornada",
        "flag_cartao_cancelado_no_momento", "flag_evento_risco", "severidade_maxima_risco",
    )
    .limit(20)
)

# COMMAND ----------

# MAGIC %md ## Evidência: gold_cliente_mes — variação mês a mês (LAG)

# COMMAND ----------

cliente_mes = spark.read.format("delta").load(config.table_path("gold", "gold_cliente_mes"))
cliente_com_mais_meses = (
    cliente_mes.groupBy("id_cliente").count().orderBy(F.desc("count")).limit(1).collect()[0]["id_cliente"]
)
display(
    cliente_mes.filter(f"id_cliente = '{cliente_com_mais_meses}'")
    .orderBy("ano_mes")
    .select("id_cliente", "ano_mes", "qtd_transacoes", "valor_liquido", "valor_liquido_mes_anterior", "variacao_valor_liquido_pct")
)

# COMMAND ----------

# MAGIC %md ## Evidência: gold_features_cliente — segmentação por percentil

# COMMAND ----------

display(
    spark.read.format("delta").load(config.table_path("gold", "gold_features_cliente"))
    .select("id_cliente", "segmento", "cidade", "valor_liquido_total", "percentil_gasto_cidade", "percentil_gasto_segmento", "decil_gasto_geral")
    .orderBy(F.desc("valor_liquido_total"))
    .limit(15)
)

# COMMAND ----------

# MAGIC %md ## Resumo de volumetria de todas as tabelas Ouro

# COMMAND ----------

for tabela in [
    "gold_fato_transacao", "gold_dim_cliente", "gold_dim_conta", "gold_dim_cartao",
    "gold_dim_estabelecimento", "gold_cliente_mes", "gold_indicadores_risco", "gold_features_cliente",
]:
    df = spark.read.format("delta").load(config.table_path("gold", tabela))
    print(f"{tabela}: {df.count()} linhas, {len(df.columns)} colunas")
