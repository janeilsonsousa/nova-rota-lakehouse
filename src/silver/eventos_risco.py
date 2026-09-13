"""Prata: eventos de risco (fraude, chargeback, suspeita) com integridade
referencial contra transações e MERGE idempotente por ``id_evento``.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.config.settings import PipelineConfig
from src.quality.rules import apply_quality_gate, write_quarantine
from src.silver.common import is_empty, read_bronze_batch
from src.utils.logging_utils import get_logger

VALID_TIPO_EVENTO = {"FRAUDE", "CHARGEBACK", "SUSPEITA"}
VALID_SEVERIDADE = {"BAIXA", "MEDIA", "ALTA", "CRITICA"}

FINAL_COLS = [
    "id_evento", "id_transacao", "tipo_evento", "severidade", "data_evento",
    "arquivo_origem", "batch_id", "timestamp_ingestao", "timestamp_processamento_silver",
]


def process_eventos_risco(spark: SparkSession, config: PipelineConfig) -> dict:
    logger = get_logger("silver.eventos_risco", batch_id=config.batch_id)
    bronze = read_bronze_batch(spark, config, "bronze_eventos_risco")
    if is_empty(bronze):
        logger.info("nenhum registro novo para processar")
        return {"entidade": "eventos_risco", "validos": 0, "quarentena": 0, "gravados": 0}

    transacoes_path = config.table_path("silver", "silver_transacoes")
    if DeltaTable.isDeltaTable(spark, transacoes_path):
        transacoes_validas = spark.read.format("delta").load(transacoes_path).select("id_transacao").distinct()
    else:
        transacoes_validas = spark.createDataFrame([], "id_transacao string")

    bronze_com_flag = bronze.join(
        transacoes_validas.withColumn("_transacao_existe", F.lit(True)), on="id_transacao", how="left"
    )

    checks = [
        ("id_evento_vazio", F.col("id_evento").isNotNull()),
        ("tipo_evento_invalido", F.col("tipo_evento").isin(list(VALID_TIPO_EVENTO))),
        ("severidade_invalida", F.col("severidade").isin(list(VALID_SEVERIDADE))),
        ("transacao_inexistente", F.col("_transacao_existe").isNotNull()),
    ]
    gated = apply_quality_gate(bronze_com_flag, checks)
    write_quarantine(spark, config, gated.quarantined.drop("_transacao_existe"), "eventos_risco")
    logger.info("quality gate aplicado", validos=gated.valid_count, quarentena=gated.quarantine_count)

    valid = gated.valid.drop("_transacao_existe")
    window = Window.partitionBy("id_evento").orderBy(F.col("timestamp_ingestao").desc(), F.col("hash_linha"))
    deduped = valid.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
    staged = deduped.withColumn("timestamp_processamento_silver", F.current_timestamp()).select(*FINAL_COLS)

    target_path = config.table_path("silver", "silver_eventos_risco")
    gravados = staged.count()
    if not DeltaTable.isDeltaTable(spark, target_path):
        staged.write.format("delta").mode("overwrite").save(target_path)
    else:
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("t")
            .merge(staged.alias("s"), "t.id_evento = s.id_evento")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    logger.info("MERGE concluído", registros_processados=gravados)
    return {
        "entidade": "eventos_risco",
        "validos": gated.valid_count,
        "quarentena": gated.quarantine_count,
        "gravados": gravados,
    }
