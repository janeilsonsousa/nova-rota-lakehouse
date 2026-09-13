"""Orquestra a camada Ouro. Ordem: fato primeiro (todo o resto depende
dela), depois dimensões (independentes entre si), depois os agregados que
leem a fato + dimensões.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.config.settings import PipelineConfig
from src.gold.cliente_mes import build_gold_cliente_mes
from src.gold.dimensions import build_gold_dimensions
from src.gold.fato_transacao import build_gold_fato_transacao
from src.gold.features_cliente import build_gold_features_cliente
from src.gold.indicadores_risco import build_gold_indicadores_risco
from src.utils.catalog import register_known_tables
from src.utils.logging_utils import get_logger

GOLD_TABLES = [
    "gold_fato_transacao", "gold_dim_cliente", "gold_dim_conta", "gold_dim_cartao",
    "gold_dim_estabelecimento", "gold_cliente_mes", "gold_indicadores_risco", "gold_features_cliente",
]


def run_gold_processing(spark: SparkSession, config: PipelineConfig) -> dict:
    logger = get_logger("gold.run", batch_id=config.batch_id)
    logger.info("iniciando processamento gold", run_mode=config.run_mode)

    fato_result = build_gold_fato_transacao(spark, config)
    dim_results = build_gold_dimensions(spark, config)
    cliente_mes_result = build_gold_cliente_mes(spark, config)
    indicadores_result = build_gold_indicadores_risco(spark, config)
    features_result = build_gold_features_cliente(spark, config)

    results = {
        "gold_fato_transacao": fato_result,
        "dimensoes": dim_results,
        "gold_cliente_mes": cliente_mes_result,
        "gold_indicadores_risco": indicadores_result,
        "gold_features_cliente": features_result,
    }
    register_known_tables(spark, config, "gold", GOLD_TABLES)
    logger.info("processamento gold finalizado", resultados=results)
    return results
