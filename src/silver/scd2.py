# SCD2 genérico pra clientes/contas/cartões. Um arquivo de CDC pode trazer mais
# de uma versão da mesma chave, então em vez de MERGE linha-a-linha (quebra com
# "multiple source rows matched"), recalcula a timeline inteira da chave afetada
# e substitui por DELETE+INSERT via MERGE. Fica idempotente e lida com dado fora
# de ordem de graça.

from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.config.settings import PipelineConfig

LINEAGE_COLS = ["arquivo_origem", "batch_id", "timestamp_ingestao"]


@dataclass(frozen=True)
class Scd2Result:
    chaves_afetadas: int
    versoes_gravadas: int


def _hash_attrs(attribute_cols: list[str]):
    return F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in attribute_cols]), 256)


def _recompute_timeline(
    combined: DataFrame, business_key: str, attribute_cols: list[str], effective_date_col: str
) -> DataFrame:
    combined = combined.withColumn("hash_atributos", _hash_attrs(attribute_cols))

    order_window = Window.partitionBy(business_key).orderBy(F.col(effective_date_col), F.col("hash_atributos"))
    with_change_flag = combined.withColumn(
        "_hash_anterior", F.lag("hash_atributos").over(order_window)
    ).withColumn(
        "_mudou", (F.col("_hash_anterior").isNull()) | (F.col("hash_atributos") != F.col("_hash_anterior"))
    )
    changed_only = with_change_flag.filter(F.col("_mudou")).drop("_hash_anterior", "_mudou")

    final_window = Window.partitionBy(business_key).orderBy(F.col(effective_date_col))
    date_type = combined.schema[effective_date_col].dataType
    # primeira versão de uma chave começa num sentinela "desde sempre", não no
    # data_atualizacao do CDC, senão transação anterior ao 1o snapshot fica sem
    # match no join ponto-no-tempo da Ouro. 1970-01-02 pra não quebrar no Windows.
    sentinel = F.lit("1970-01-02T00:00:00").cast(date_type)
    timeline = (
        changed_only.withColumn("versao", F.row_number().over(final_window))
        .withColumn(
            "dt_inicio_vigencia",
            F.when(F.col("versao") == 1, sentinel).otherwise(F.col(effective_date_col)),
        )
        .withColumn("dt_fim_vigencia", F.lead(F.col(effective_date_col)).over(final_window))
        .withColumn("flag_vigente", F.col("dt_fim_vigencia").isNull())
    )
    return timeline


def apply_scd2(
    spark: SparkSession,
    config: PipelineConfig,
    table_path: str,
    incoming: DataFrame,
    business_key: str,
    attribute_cols: list[str],
    effective_date_col: str = "data_atualizacao",
) -> Scd2Result:
    # incoming já passou pelo quality gate e tem as colunas de lineage
    output_cols = [business_key, *attribute_cols, effective_date_col, *LINEAGE_COLS]
    incoming_slim = incoming.select(*output_cols)

    affected_keys_df = incoming_slim.select(business_key).distinct()

    if DeltaTable.isDeltaTable(spark, table_path):
        target = DeltaTable.forPath(spark, table_path)
        existing_affected = (
            target.toDF()
            .join(affected_keys_df, on=business_key, how="inner")
            .select(*output_cols)
        )
        combined = existing_affected.unionByName(incoming_slim)
    else:
        target = None
        combined = incoming_slim

    timeline = _recompute_timeline(combined, business_key, attribute_cols, effective_date_col)
    timeline = timeline.withColumn("timestamp_processamento_silver", F.current_timestamp())

    final_cols = [
        business_key, *attribute_cols, effective_date_col,
        "dt_inicio_vigencia", "dt_fim_vigencia", "flag_vigente", "versao", "hash_atributos",
        *LINEAGE_COLS, "timestamp_processamento_silver",
    ]
    timeline = timeline.select(*final_cols)
    versoes_gravadas = timeline.count()
    chaves_afetadas = timeline.select(business_key).distinct().count()

    if target is None:
        timeline.write.format("delta").mode("overwrite").save(table_path)
    else:
        staged = (
            target.toDF()
            .join(affected_keys_df, on=business_key, how="inner")
            .select(F.col(business_key).alias("mergeKey"), *[F.col(c) for c in final_cols])
            .unionByName(
                timeline.select(F.lit(None).cast(timeline.schema[business_key].dataType).alias("mergeKey"), *final_cols)
            )
        )
        (
            target.alias("t")
            .merge(staged.alias("s"), f"t.{business_key} = s.mergeKey")
            .whenMatchedDelete()
            .whenNotMatchedInsert(values={c: f"s.{c}" for c in final_cols})
            .execute()
        )

    return Scd2Result(chaves_afetadas=chaves_afetadas, versoes_gravadas=versoes_gravadas)


def read_scd2_current(spark: SparkSession, table_path: str) -> DataFrame:
    return spark.read.format("delta").load(table_path).filter(F.col("flag_vigente"))
