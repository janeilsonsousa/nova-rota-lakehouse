"""Motor de regras de qualidade com segregação em quarentena.

O padrão é o mesmo para todas as entidades da Prata: cada regra é uma
condição booleana ("verdadeiro quando o registro é válido"); um registro que
falha em pelo menos uma regra vai inteiro para a tabela de quarentena da
entidade, com a lista de motivos que o reprovaram — nada é descartado
silenciosamente, e o time de dados consegue auditar exatamente por que um
registro não chegou na Prata.
"""

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
    """Aplica um conjunto de regras (motivo, condição_de_validade).

    Um registro que falha em qualquer condição é marcado com a coluna
    ``motivos_quarentena`` (array de strings, uma por regra reprovada) e
    segregado do resultado válido. Nenhuma linha é descartada: toda entrada
    aparece em ``valid`` XOR ``quarantined``.
    """
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
    # limit(1).count() em vez de rdd.isEmpty(): isEmpty() força uma coleta
    # real de linhas para o driver via worker Python (take(1)), que é
    # instável nesta execução local Windows (ver docs/decisions.md).
    # limit(1).count() é um agregado puro na JVM — nunca aciona o worker.
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
    """Remove duplicatas exatas (mesma chave + mesmo timestamp de
    atualização) mantendo 1 registro determinístico via ROW_NUMBER.

    Usado quando a origem manda o mesmo evento duas vezes no mesmo lote
    (ex.: cliente C0005 com 2 linhas idênticas em ``clientes_cdc.csv``).
    """
    from pyspark.sql import Window

    window = Window.partitionBy(*key_cols, order_col).orderBy(F.col("hash_linha"))
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
