"""Prata: dimensão de contas com histórico SCD Tipo 2 e integridade
referencial com clientes.
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

ATTRIBUTE_COLS = ["id_cliente", "tipo_conta", "status_conta", "data_abertura"]
VALID_STATUS = {"ATIVA", "ENCERRADA"}


def process_contas(spark: SparkSession, config: PipelineConfig) -> dict:
    logger = get_logger("silver.contas", batch_id=config.batch_id)
    bronze = read_bronze_batch(spark, config, "bronze_contas_cdc")
    if is_empty(bronze):
        logger.info("nenhum registro novo para processar")
        return {"entidade": "contas", "validos": 0, "quarentena": 0, "chaves_afetadas": 0}

    clientes_path = config.table_path("silver", "silver_clientes")
    if DeltaTable.isDeltaTable(spark, clientes_path):
        clientes_validos = spark.read.format("delta").load(clientes_path).select("id_cliente").distinct()
    else:
        clientes_validos = spark.createDataFrame([], "id_cliente string")

    bronze_com_flag_cliente = bronze.join(
        clientes_validos.withColumn("_cliente_existe", F.lit(True)), on="id_cliente", how="left"
    )

    checks = [
        ("id_conta_vazio", F.col("id_conta").isNotNull()),
        ("status_invalido", F.col("status_conta").isin(list(VALID_STATUS))),
        ("cliente_inexistente", F.col("_cliente_existe").isNotNull()),
    ]
    gated = apply_quality_gate(bronze_com_flag_cliente, checks)
    write_quarantine(spark, config, gated.quarantined.drop("_cliente_existe"), "contas")
    logger.info("quality gate aplicado", validos=gated.valid_count, quarentena=gated.quarantine_count)

    valid = gated.valid.drop("_cliente_existe")
    deduped = dedupe_exact_duplicates(valid, ["id_conta"], "data_atualizacao")

    result = apply_scd2(
        spark, config,
        config.table_path("silver", "silver_contas"),
        deduped,
        business_key="id_conta",
        attribute_cols=ATTRIBUTE_COLS,
    )
    logger.info("SCD2 aplicado", chaves_afetadas=result.chaves_afetadas, versoes_gravadas=result.versoes_gravadas)
    return {
        "entidade": "contas",
        "validos": gated.valid_count,
        "quarentena": gated.quarantine_count,
        "chaves_afetadas": result.chaves_afetadas,
    }
