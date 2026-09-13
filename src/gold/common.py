"""Utilitários compartilhados pela camada Ouro.

O principal é ``join_ponto_no_tempo``: resolve, para cada linha de fato
(com uma data de referência, normalmente ``data_transacao``), qual era a
versão *vigente naquele instante* de uma dimensão SCD2 — não a versão atual.
É a implementação direta do requisito "dados cadastrais devem refletir a
versão vigente na data da transação".
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

SEVERIDADE_RANK = {"BAIXA": 1, "MEDIA": 2, "ALTA": 3, "CRITICA": 4}


def join_ponto_no_tempo(
    fatos: DataFrame,
    dimensao_scd2: DataFrame,
    chave: str,
    data_col: str,
    prefixo: str,
) -> DataFrame:
    """Junta ``fatos`` (1 linha por evento, com ``data_col``) à versão da
    dimensão SCD2 vigente em ``data_col`` — join por intervalo, não join
    simples por chave. Colunas trazidas da dimensão são prefixadas com
    ``prefixo`` para evitar colisão de nomes.
    """
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
    """Lê uma tabela Silver; se ela ainda não existe (ex.: nenhum arquivo de
    eventos_risco/estornos chegou ainda para este ambiente), devolve um
    DataFrame vazio com as colunas mínimas pedidas — os agregados da Ouro
    tratam isso como "zero eventos/zero estornos" em vez de falhar.
    """
    if DeltaTable.isDeltaTable(spark, path):
        return spark.read.format("delta").load(path)
    schema = StructType([StructField(c, StringType()) for c in columns])
    return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)


def merge_by_composite_key(
    spark: SparkSession,
    target_path: str,
    staged: DataFrame,
    key_cols: list[str],
    final_cols: list[str],
) -> None:
    """Substitui, por chave composta, o conteúdo de uma tabela agregada
    (ex.: ``id_cliente + ano_mes``) via MERGE INTO (delete das linhas
    antigas da chave + insert das recalculadas). Usado por agregados Ouro
    onde a métrica não é somável incrementalmente (ticket médio, contagem
    distinta) — o grupo inteiro precisa ser recalculado quando qualquer
    membro dele muda.
    """
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
