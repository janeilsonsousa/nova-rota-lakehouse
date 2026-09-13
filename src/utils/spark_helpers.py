"""Helpers genéricos de construção de DataFrame.

``build_literal_df`` existe por causa de DUAS incompatibilidades de
ambiente diferentes que, juntas, eliminam qualquer caminho baseado em RDD
ou em coleção Python bruta (ver ADRs em ``docs/decisions.md``):

1. Local, Windows (dev/teste): ``spark.createDataFrame(<lista python>, schema)``
   deveria ser a forma normal de materializar um punhado de linhas geradas
   no driver, mas com PySpark 3.5.1 + Python 3.12 esse caminho
   (``SparkContext.parallelize`` + worker Python desserializando dados
   "pickled") derruba o worker Python com um ``EOFException`` pouco
   descritivo.
2. Databricks Serverless: acesso direto a ``spark.sparkContext``/RDD é
   bloqueado por design (``JVM_ATTRIBUTE_NOT_SUPPORTED`` — isolamento de
   compute compartilhado), então a saída óbvia para o problema 1
   (``spark.sparkContext.emptyRDD()``) quebra especificamente ali.

A solução usa só a API de alto nível do DataFrame — nunca RDD, nunca uma
coleção Python é enviada ao Spark: cada linha vira
``spark.range(1).select(lit(...))`` (uma operação 100% JVM/Catalyst) unida
via ``unionByName``; o caso vazio vira ``spark.range(0).select(lit(...))``
(mesmo princípio, zero linhas). Funciona sem alteração em execução local
Windows, em Databricks com cluster clássico e em Databricks Serverless.
"""

from __future__ import annotations

from functools import reduce
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def build_literal_df(spark: SparkSession, rows: list[dict[str, Any]], schema: StructType) -> DataFrame:
    if not rows:
        empty_cols = [F.lit(None).cast(field.dataType).alias(field.name) for field in schema.fields]
        return spark.range(0).select(*empty_cols)

    def _row_df(row: dict[str, Any]) -> DataFrame:
        cols = [F.lit(row[field.name]).cast(field.dataType).alias(field.name) for field in schema.fields]
        return spark.range(1).select(*cols)

    return reduce(lambda a, b: a.unionByName(b), (_row_df(r) for r in rows))
