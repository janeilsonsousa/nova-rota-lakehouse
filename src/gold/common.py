from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.utils.spark_helpers import build_literal_df

SEVERIDADE_RANK = {"BAIXA": 1, "MEDIA": 2, "ALTA": 3, "CRITICA": 4}


def join_ponto_no_tempo(
    fatos: DataFrame,
    dimensao_scd2: DataFrame,
    chave: str,
    data_col: str,
    prefixo: str,
) -> DataFrame:
    # join por intervalo: pega a versão da dimensão vigente na data_col, não a atual
    excluidas = (chave, "dt_inicio_vigencia", "dt_fim_vigencia")
    dim = dimensao_scd2.select(
        F.col(chave).alias(f"_{chave}"),
        *[F.col(c).alias(f"{prefixo}{c}") for c in dimensao_scd2.columns if c not in excluidas],
        F.col("dt_inicio_vigencia").alias("_dt_inicio"),
        F.col("dt_fim_vigencia").alias("_dt_fim"),
    )
    condicao = (
        (F.col(chave) == F.col(f"_{chave}"))
        & (F.col(data_col) >= F.col("_dt_inicio"))
        & (F.col("_dt_fim").isNull() | (F.col(data_col) < F.col("_dt_fim")))
    )
    joined = fatos.join(dim, condicao, "left").drop(f"_{chave}", "_dt_inicio", "_dt_fim")
    return joined


def severidade_rank_col(col_name: str) -> F.Column:
    mapping = F.create_map(*[F.lit(x) for pair in SEVERIDADE_RANK.items() for x in pair])
    return mapping[F.col(col_name)]


def read_silver_or_empty(spark: SparkSession, path: str, columns: list[str]) -> DataFrame:
    # se a tabela ainda não existe, devolve vazio em vez de quebrar o agregado
    if DeltaTable.isDeltaTable(spark, path):
        return spark.read.format("delta").load(path)
    schema = StructType([StructField(c, StringType()) for c in columns])
    return build_literal_df(spark, [], schema)


def merge_by_composite_key(
    spark: SparkSession,
    target_path: str,
    staged: DataFrame,
    key_cols: list[str],
    final_cols: list[str],
) -> None:
    # recalcula e substitui o grupo inteiro (chave composta) via delete+insert
    if not DeltaTable.isDeltaTable(spark, target_path):
        staged.write.format("delta").mode("overwrite").save(target_path)
        return

    target = DeltaTable.forPath(spark, target_path)
    merge_key_expr_target = F.concat_ws("||", *[F.col(f"t.{c}") for c in key_cols])
    merge_key_expr_staged = F.concat_ws("||", *key_cols)

    existing_affected = (
        target.toDF()
        .join(staged.select(*key_cols).distinct(), key_cols, "inner")
        .withColumn("mergeKey", merge_key_expr_staged)
        .select("mergeKey", *final_cols)
    )
    staged_keyed = staged.withColumn("mergeKey", F.lit(None).cast("string")).select("mergeKey", *final_cols)
    merge_source = existing_affected.unionByName(staged_keyed)

    (
        target.alias("t")
        .merge(merge_source.alias("s"), merge_key_expr_target == F.col("s.mergeKey"))
        .whenMatchedDelete()
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in final_cols})
        .execute()
    )
