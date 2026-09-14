# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00 — Setup do ambiente
# MAGIC
# MAGIC Cria o catálogo/schemas Unity Catalog (ou o database local, se não
# MAGIC houver Unity Catalog disponível no workspace) e valida que o
# MAGIC repositório (`src/`) está importável.
# MAGIC
# MAGIC Rode este notebook uma vez por workspace, antes do `01_bronze_ingestion`.

# COMMAND ----------

# Setup inicial do ambiente
import sys
from pathlib import Path

repo_root = Path.cwd()

while not (repo_root / "src").exists() and repo_root != repo_root.parent:
    repo_root = repo_root.parent

if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from src.config import get_config  # noqa: E402

CATALOG = "nova_rota"

print("=" * 60)
print("NovaRota Lakehouse - Setup do ambiente")
print("=" * 60)
print(f"Repositório encontrado : {repo_root}")
print(f"Catálogo Databricks    : {CATALOG}")
print(f"Python                 : {sys.version.split()[0]}")
print("Import src.config      : OK")
print("Status                 : Ambiente preparado com sucesso")
print("=" * 60)

# COMMAND ----------

# MAGIC %md ## Unity Catalog
# MAGIC
# MAGIC Se o workspace tiver Unity Catalog habilitado, cria o catálogo e os 3
# MAGIC schemas. Se não (ex. workspace sem metastore UC), cai para databases
# MAGIC no `hive_metastore`/`spark_catalog` — o pipeline funciona nos dois
# MAGIC casos porque `PipelineConfig` só muda o *nome* qualificado da tabela,
# MAGIC não a lógica.

# COMMAND ----------

# Aqui eu valido se existe o Catalog disponível, caso não exista, será criado, no final tem uma mensagem informando se o Unity Catalog está disponível ou não.
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

# MAGIC %md
# MAGIC ## Armazenamento das tabelas Delta
# MAGIC
# MAGIC O pipeline grava as tabelas Delta usando o caminho onde os arquivos ficam armazenados.
# MAGIC Dessa forma, o mesmo código pode ser executado localmente ou no Databricks sem precisar mudar a lógica do pipeline.
# MAGIC No Databricks, os dados não ficam dentro da pasta `/Workspace`, porque esse local é voltado para notebooks e arquivos do projeto.
# MAGIC Por isso, as tabelas são gravadas em um Volume do Unity Catalog, que é o local utilizado para armazenar os dados do pipeline.

# COMMAND ----------

# Inclusive eu coloquei aqui uma validação de storagem, caso exista ou não um volume de armazenamento.
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
