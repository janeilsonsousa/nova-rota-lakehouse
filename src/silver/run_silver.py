from __future__ import annotations

from pyspark.sql import SparkSession

from src.config.settings import PipelineConfig
from src.silver.cartoes import process_cartoes
from src.silver.clientes import process_clientes
from src.silver.contas import process_contas
from src.silver.estornos import process_estornos
from src.silver.eventos_risco import process_eventos_risco
from src.silver.transacoes import process_transacoes
from src.utils.catalog import register_known_tables
from src.utils.logging_utils import get_logger

SILVER_TABLES = [
    "silver_clientes", "silver_contas", "silver_cartoes",
    "silver_transacoes", "silver_eventos_risco", "silver_estornos",
]


def run_silver_processing(spark: SparkSession, config: PipelineConfig) -> list[dict]:
    logger = get_logger("silver.run", batch_id=config.batch_id)
    logger.info("iniciando processamento silver", run_mode=config.run_mode)

    # ordem importa: clientes -> contas -> cartoes -> transacoes -> eventos_risco/estornos
    results = [
        process_clientes(spark, config),
        process_contas(spark, config),
        process_cartoes(spark, config),
        process_transacoes(spark, config),
        process_eventos_risco(spark, config),
        process_estornos(spark, config),
    ]

    register_known_tables(spark, config, "silver", SILVER_TABLES)
    logger.info("processamento silver finalizado", resultados=results)
    return results
