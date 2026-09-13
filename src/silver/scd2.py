"""SCD Tipo 2 genérico para dimensões cadastrais (clientes, contas, cartões).

Por que SCD2 (e não overwrite ou SCD1)
---------------------------------------
O desafio pede explicitamente histórico de dimensões e que "dados cadastrais
devem refletir a versão vigente na data da transação" — isso só é possível
com histórico versionado (SCD2). Um SCD1 (sobrescreve o atributo mais
recente) perderia a capacidade de saber, por exemplo, qual era o status de
um cartão quando uma transação de 3 meses atrás aconteceu.

Por que reconstrução de timeline (delete+insert via MERGE) em vez de
MERGE linha-a-linha
----------------------------------------------------------------------
Os arquivos de CDC deste desafio não trazem "1 evento por execução" — um
único arquivo (ex.: ``clientes_cdc.csv``) pode conter *múltiplas versões
históricas da mesma chave* (o mesmo cliente atualizado 2x, cada linha com um
``data_atualizacao`` diferente). Um MERGE INTO tradicional (1 UPDATE de
fechamento + 1 INSERT de abertura por chave) falha com
"multiple source rows matched" quando o lote de origem tem mais de uma linha
por chave.

A solução adotada: para cada chave afetada pelo lote, juntamos o histórico
já existente na Prata (se houver) com as novas versões do lote, recalculamos
a timeline inteira (``dt_inicio_vigencia``/``dt_fim_vigencia``/``flag_vigente``/
``versao``) com funções de janela (LEAD/ROW_NUMBER) e substituímos o
histórico daquela chave por inteiro via **MERGE INTO** (DELETE das versões
antigas da chave + INSERT das versões recalculadas). Isso garante:

- **Idempotência**: reexecutar o mesmo lote produz exatamente o mesmo
  resultado (a timeline é recalculada do zero a cada vez a partir da união
  histórico+novo, não incrementada por soma).
- **Correção com dados fora de ordem**: uma versão "atrasada" (data de
  atualização anterior à última versão já vigente) é reencaixada na posição
  cronológica correta porque a timeline inteira é reordenada por
  ``data_atualizacao``, não apenas apensada no fim.
- **Robustez a reenvio sem mudança real**: versões consecutivas com o mesmo
  hash de atributos (reenvio do mesmo CDC sem alteração de valor) são
  colapsadas em uma única versão, evitando histórico "ruído".
"""

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
    # A primeira versão conhecida de uma chave recebe dt_inicio_vigencia num
    # sentinela "desde sempre", não o data_atualizacao do evento de CDC que a
    # trouxe. Por quê: data_atualizacao é a data em que o sistema de origem
    # *registrou* aquele estado, não necessariamente a data em que ele
    # passou a ser verdade — um cliente/conta/cartão pode já existir e ter
    # transações antes do primeiro snapshot de CDC que o sistema nos enviou.
    # Sem esse sentinela, qualquer transação anterior ao primeiro
    # data_atualizacao ficaria sem correspondência no join ponto-no-tempo da
    # Ouro (bug real encontrado e corrigido durante o desenvolvimento — ver
    # docs/decisions.md). Usamos 1970-01-02 (não 1900-01-01): timestamp
    # pré-epoch quebra datetime.fromtimestamp() no Windows ao fazer collect()
    # em execução/teste local — 1970 já é "infinitamente antigo" para os
    # dados deste domínio (que começam em 2024+) e funciona nos dois SOs.
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
    """Aplica (ou inicializa) o histórico SCD2 para as chaves presentes em
    ``incoming``. ``incoming`` já deve ter passado pelo quality gate (só
    contém registros válidos) e conter as colunas de lineage (LINEAGE_COLS).
    """
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
    """Retorna apenas a versão vigente (flag_vigente=true) de cada chave."""
    return spark.read.format("delta").load(table_path).filter(F.col("flag_vigente"))


def read_scd2_as_of(spark: SparkSession, table_path: str, business_key: str, as_of_col: str) -> DataFrame:
    """Retorna o DataFrame completo (todas as versões) para permitir join
    ponto-no-tempo em ``as_of_col BETWEEN dt_inicio_vigencia AND dt_fim_vigencia``.
    """
    return spark.read.format("delta").load(table_path)
