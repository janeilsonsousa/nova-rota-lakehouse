"""Helpers genéricos de construção de DataFrame.

``build_literal_df`` existe por causa de uma incompatibilidade específica de
ambiente (ver ADR "Execução local no Windows" em ``docs/decisions.md``):
``spark.createDataFrame(<lista python>, schema)`` deveria ser a forma normal
de materializar um punhado de linhas geradas no driver (ex.: registros de
controle de ingestão), mas em execução local no Windows com PySpark 3.5.1 +
Python 3.12 esse caminho (``SparkContext.parallelize`` + worker Python que
desserializa os dados "pickled") derruba o worker Python com um
``EOFException`` pouco descritivo.

A alternativa abaixo constrói cada linha via ``spark.range(1).select(lit(...))``
— uma operação 100% nativa da JVM/Catalyst que nunca aciona um worker
Python — e une as linhas com ``unionByName``. Isso é equivalente em
resultado ao ``createDataFrame`` para os volumes pequenos em que é usado
(metadados de controle, poucas linhas por execução) e é totalmente portátil:
em Databricks o ``createDataFrame`` normal funcionaria sem problema, então
esta função é apenas uma camada de segurança que funciona nos dois
ambientes.
"""

from __future__ import annotations

from functools import reduce
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def build_literal_df(spark: SparkSession, rows: list[dict[str, Any]], schema: StructType) -> DataFrame:
    if not rows:
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    def _row_df(row: dict[str, Any]) -> DataFrame:
        cols = [F.lit(row[field.name]).cast(field.dataType).alias(field.name) for field in schema.fields]
        return spark.range(1).select(*cols)

    return reduce(lambda a, b: a.unionByName(b), (_row_df(r) for r in rows))
