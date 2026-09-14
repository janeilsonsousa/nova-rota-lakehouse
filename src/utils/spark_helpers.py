# build_literal_df evita spark.createDataFrame(lista, schema) e RDD: quebra no
# Windows local (EOFException) e no Databricks Serverless (RDD bloqueado). Monta
# cada linha via spark.range(1).select(lit(...)) + unionByName em vez disso.

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
