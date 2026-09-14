# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup do ambiente
# MAGIC Cria catalogo/schemas e valida que o src/ importa. Roda 1x por workspace antes do 01_bronze_ingestion.

# COMMAND ----------

import sys
from pathlib import Path

# fallback pra quando o notebook roda fora de um Databricks Repo
repo_root = Path.cwd()
while not (repo_root / "src").exists() and repo_root != repo_root.parent:
    repo_root = repo_root.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.config import get_config  # noqa: E402

CATALOG = "nova_rota"

# COMMAND ----------

# MAGIC %md ## Unity Catalog (se disponível)
# MAGIC Cria o catálogo + 3 schemas. Sem UC, cai pra database no metastore default.

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"USE CATALOG {CATALOG}")
    unity_catalog_disponivel = True
except Exception as e:
    print(f"Unity Catalog não disponível neste workspace ({e}); usando schemas no catálogo default.")
    unity_catalog_disponivel = False

for schema in ("bronze", "silver", "gold"):
    if unity_catalog_disponivel:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")

print("Ambiente pronto. unity_catalog_disponivel =", unity_catalog_disponivel)

# COMMAND ----------

# MAGIC %md ## Volume de armazenamento
# MAGIC O pipeline grava por path físico, não por nome de catálogo. O Workspace é read-only pra dado, então usa um Volume UC (ou DBFS).

# COMMAND ----------

VOLUME_BASE_PATH = None
if unity_catalog_disponivel:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.storage")
    VOLUME_BASE_PATH = f"/Volumes/{CATALOG}/bronze/storage"
    print("Volume de armazenamento pronto em:", VOLUME_BASE_PATH)
else:
    print("Sem Unity Catalog: use um path gravável (ex. dbfs:/tmp/...) como --base-path.")

# COMMAND ----------

# MAGIC %md ## Validação rápida

# COMMAND ----------

config = get_config(env="databricks", catalog=CATALOG)
print(config.as_dict())
assert config.table_fqn("gold", "gold_fato_transacao") == f"{CATALOG}.gold.gold_fato_transacao"
print("OK — configuração validada.")
