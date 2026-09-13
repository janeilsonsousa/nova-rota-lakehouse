"""Registro de tabelas Delta (escritas por path) no catálogo Unity Catalog.

O pipeline escreve/lê todas as tabelas por **path físico**
(`config.table_path(...)`), não por nome de catálogo — isso é o que torna o
mesmo código 100% portátil entre execução local (sem metastore) e
Databricks. Mas para que `sql/advanced_queries.sql`, o notebook
`04_sql_analysis` e qualquer analista no Catalog Explorer consigam fazer
`SELECT ... FROM nova_rota.gold.gold_fato_transacao`, a tabela física
precisa também existir como tabela registrada no catálogo.

Por que CREATE TABLE ... AS SELECT (CTAS), não CREATE TABLE ... LOCATION
--------------------------------------------------------------------------
A abordagem mais óbvia — registrar a tabela do catálogo apontando (por
`LOCATION`) direto para o path físico, sem duplicar dado — **não funciona**
quando esse path fica dentro de um Volume Unity Catalog: Volumes servem
para arquivos (dado bruto, checkpoints, quarentena), não são aceitos como
`LOCATION` de tabela registrada (o erro é
``INVALID_PARAMETER_VALUE: Missing cloud file system scheme`` — Unity
Catalog exige um External Location com storage credential próprio para
apontar para um path arbitrário, fora do escopo de uma conta pessoal
gratuita).

A alternativa adotada é `CREATE OR REPLACE TABLE ... AS SELECT * FROM
delta.\`{path}\``: cria (ou atualiza) uma **managed table** do Unity
Catalog — o Catalog Explorer already, `INFORMATION_SCHEMA` e qualquer
query por nome funcionam normalmente. O custo é uma cópia física do dado a
cada execução; para o volume deste pipeline (centenas de linhas) isso é
desprezível. Em produção com um External Location configurado, a versão
por `LOCATION` seria preferível (evita a cópia); a função tenta as duas,
nessa ordem, e cai para a segunda quando a primeira falhar por essa razão
específica.

Esta função só roda em ``env == "databricks"`` (em execução local não há
necessidade de catálogo — os testes/scripts locais leem direto por path).
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
        return
    except Exception:
        pass  # Volume não aceita LOCATION — cai para CTAS (managed table) abaixo.
    try:
        spark.sql(f"CREATE OR REPLACE TABLE {fqn} AS SELECT * FROM delta.`{path}`")
    except Exception:
        # Best-effort: o pipeline já lê/escreve por path físico e não
        # depende deste registro para funcionar — ele só existe para
        # conveniência de consulta SQL por nome. Ver docs/decisions.md.
        pass


def register_known_tables(spark: SparkSession, config: PipelineConfig, layer: str, tables: list[str]) -> None:
    for table in tables:
        register_delta_table(spark, config, layer, table)
