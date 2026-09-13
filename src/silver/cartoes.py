"""Prata: dimensão de cartões com histórico SCD Tipo 2 e integridade
referencial com contas.

Cartões cancelados preservam histórico (o SCD2 nunca apaga versões
anteriores) e são excluídos de métricas futuras na camada Ouro através do
filtro por ``status_cartao`` vigente no momento de cada transação — não
aqui, na Prata, que é responsável apenas por manter os dados corretos e
completos.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config.settings import PipelineConfig
from src.quality.rules import apply_quality_gate, dedupe_exact_duplicates, write_quarantine
from src.silver.common import is_empty, read_bronze_batch
from src.silver.scd2 import apply_scd2
from src.utils.logging_utils import get_logger

ATTRIBUTE_COLS = ["id_conta", "tipo_cartao", "limite", "status_cartao"]
VALID_STATUS = {"ATIVO", "CANCELADO"}


def process_cartoes(spark: SparkSession, config: PipelineConfig) -> dict:
    logger = get_logger("silver.cartoes", batch_id=config.batch_id)
    bronze = read_bronze_batch(spark, config, "bronze_cartoes_cdc")
    if is_empty(bronze):
        logger.info("nenhum registro novo para processar")
        return {"entidade": "cartoes", "validos": 0, "quarentena": 0, "chaves_afetadas": 0}

    contas_path = config.table_path("silver", "silver_contas")
    if DeltaTable.isDeltaTable(spark, contas_path):
        contas_validas = spark.read.format("delta").load(contas_path).select("id_conta").distinct()
    else:
        contas_validas = spark.createDataFrame([], "id_conta string")

    bronze_com_flag_conta = bronze.join(
        contas_validas.withColumn("_conta_existe", F.lit(True)), on="id_conta", how="left"
    )

    checks = [
        ("id_cartao_vazio", F.col("id_cartao").isNotNull()),
        ("status_invalido", F.col("status_cartao").isin(list(VALID_STATUS))),
        ("limite_negativo", F.col("limite") >= F.lit(0)),
        ("conta_inexistente", F.col("_conta_existe").isNotNull()),
    ]
    gated = apply_quality_gate(bronze_com_flag_conta, checks)
    write_quarantine(spark, config, gated.quarantined.drop("_conta_existe"), "cartoes")
    logger.info("quality gate aplicado", validos=gated.valid_count, quarentena=gated.quarantine_count)

    valid = gated.valid.drop("_conta_existe")
    deduped = dedupe_exact_duplicates(valid, ["id_cartao"], "data_atualizacao")

    result = apply_scd2(
        spark, config,
        config.table_path("silver", "silver_cartoes"),
        deduped,
        business_key="id_cartao",
        attribute_cols=ATTRIBUTE_COLS,
    )
    logger.info("SCD2 aplicado", chaves_afetadas=result.chaves_afetadas, versoes_gravadas=result.versoes_gravadas)
    return {
        "entidade": "cartoes",
        "validos": gated.valid_count,
        "quarentena": gated.quarantine_count,
        "chaves_afetadas": result.chaves_afetadas,
    }
