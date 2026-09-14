# Regras de qualidade com quarentena. Falhou alguma regra, vai pra quarentena com o motivo.

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config.settings import PipelineConfig

VALID_UF = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
}

CPF_FORMAT_REGEX = r"^\d{3}\.\d{3}\.\d{3}-\d{2}$"


@dataclass(frozen=True)
class QualityResult:
    valid: DataFrame
    quarantined: DataFrame
    quarantine_count: int
    valid_count: int


def apply_quality_gate(
    df: DataFrame,
    checks: list[tuple[str, F.Column]],
    id_cols: list[str] | None = None,
) -> QualityResult:
    fail_flags = [F.when(~cond, F.lit(motivo)) for motivo, cond in checks]
    with_flags = df.withColumn("_motivos", F.array_compact(F.array(*fail_flags)))

    quarantined = (
        with_flags.filter(F.size("_motivos") > 0)
        .withColumn("motivos_quarentena", F.concat_ws("; ", "_motivos"))
        .drop("_motivos")
    )
    valid = with_flags.filter(F.size("_motivos") == 0).drop("_motivos")

    valid_count = valid.count()
    quarantine_count = quarantined.count()

    return QualityResult(
        valid=valid, quarantined=quarantined, quarantine_count=quarantine_count, valid_count=valid_count
    )


def write_quarantine(
    spark: SparkSession,
    config: PipelineConfig,
    quarantined: DataFrame,
    entidade: str,
) -> None:
    # limit(1).count() em vez de isEmpty() -- isEmpty() é instável no Windows local
    if quarantined.limit(1).count() == 0:
        return
    enriched = (
        quarantined.withColumn("entidade", F.lit(entidade))
        .withColumn("batch_id", F.lit(config.batch_id))
        .withColumn("timestamp_quarentena", F.current_timestamp())
    )
    path = f"{config.quarantine_path}/{entidade}"
    enriched.write.format("delta").mode("append").option("mergeSchema", "true").save(path)


def dedupe_exact_duplicates(df: DataFrame, key_cols: list[str], order_col: str) -> DataFrame:
    # remove linha duplicada exata no mesmo lote (ex.: cliente repetido 2x no CSV)
    from pyspark.sql import Window

    window = Window.partitionBy(*key_cols, order_col).orderBy(F.col("hash_linha"))
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
