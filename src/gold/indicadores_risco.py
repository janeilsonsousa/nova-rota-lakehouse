"""Ouro: ``gold_indicadores_risco`` — visão consolidada de eventos de risco,
estornos e chargebacks por cliente e mês.

Granularidade e chaves
-----------------------
- **Grão**: 1 linha por (``id_cliente``, ``ano_mes``) — mesmo grão de
  ``gold_cliente_mes`` para permitir join 1:1 direto entre comportamento e
  risco na mesma consulta.
- **Fonte**: ``gold_fato_transacao`` (contagens agregadas por transação) +
  ``silver_eventos_risco`` (quebra por tipo de evento, que não está
  denormalizada na fato para não incrementar o grão dela).
- **taxa_estorno**: ``qtd_estornos / qtd_transacoes`` do mês — indicador
  direto de propensão a estorno, útil como feature para modelos de risco.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config.settings import PipelineConfig
from src.gold.common import merge_by_composite_key, read_silver_or_empty
from src.silver.common import is_empty
from src.utils.logging_utils import get_logger

FINAL_COLS = [
    "id_cliente", "ano_mes",
    "qtd_transacoes", "qtd_eventos_risco", "qtd_fraude", "qtd_chargeback", "qtd_suspeita",
    "qtd_estornos", "valor_estornado", "taxa_estorno_pct", "severidade_maxima_mes",
    "batch_id", "timestamp_processamento_gold",
]

_SEVERIDADE_ORDEM = {"BAIXA": 1, "MEDIA": 2, "ALTA": 3, "CRITICA": 4}


def build_gold_indicadores_risco(spark: SparkSession, config: PipelineConfig) -> dict:
    logger = get_logger("gold.indicadores_risco", batch_id=config.batch_id)
    fato = spark.read.format("delta").load(config.table_path("gold", "gold_fato_transacao"))
    eventos = read_silver_or_empty(
        spark, config.table_path("silver", "silver_eventos_risco"), ["id_transacao", "tipo_evento", "severidade"]
    )

    fato_com_mes = fato.withColumn("ano_mes", F.date_format(F.col("dt_transacao"), "yyyy-MM")).withColumnRenamed(
        "id_cliente_na_data", "id_cliente"
    )

    if config.run_mode != "full":
        affected = (
            fato_com_mes.filter(F.col("batch_id") == config.batch_id).select("id_cliente", "ano_mes").distinct()
        )
        if is_empty(affected):
            logger.info("nenhum (cliente, mês) afetado nesta execução")
            return {"tabela": "gold_indicadores_risco", "gravados": 0}
        fato_escopo = fato_com_mes.join(affected, ["id_cliente", "ano_mes"], "inner")
    else:
        fato_escopo = fato_com_mes

    base = fato_escopo.groupBy("id_cliente", "ano_mes").agg(
        F.count("*").alias("qtd_transacoes"),
        F.sum(F.when(F.col("flag_estornada"), 1).otherwise(0)).alias("qtd_estornos"),
        F.sum(F.when(F.col("flag_estornada"), F.col("valor")).otherwise(0.0)).alias("valor_estornado"),
    )

    eventos_com_mes = eventos.join(
        fato.select(
            F.col("id_transacao"),
            F.col("id_cliente_na_data").alias("id_cliente"),
            F.date_format(F.col("dt_transacao"), "yyyy-MM").alias("ano_mes"),
        ),
        "id_transacao",
        "inner",
    )
    eventos_agg = eventos_com_mes.groupBy("id_cliente", "ano_mes").agg(
        F.count("*").alias("qtd_eventos_risco"),
        F.sum(F.when(F.col("tipo_evento") == "FRAUDE", 1).otherwise(0)).alias("qtd_fraude"),
        F.sum(F.when(F.col("tipo_evento") == "CHARGEBACK", 1).otherwise(0)).alias("qtd_chargeback"),
        F.sum(F.when(F.col("tipo_evento") == "SUSPEITA", 1).otherwise(0)).alias("qtd_suspeita"),
        F.max(
            F.create_map(*[F.lit(x) for pair in _SEVERIDADE_ORDEM.items() for x in pair])[F.col("severidade")]
        ).alias("_rank_max"),
    )
    rank_to_label = F.create_map(*[F.lit(x) for pair in {v: k for k, v in _SEVERIDADE_ORDEM.items()}.items() for x in pair])
    eventos_agg = eventos_agg.withColumn("severidade_maxima_mes", rank_to_label[F.col("_rank_max")]).drop("_rank_max")

    result = base.join(eventos_agg, ["id_cliente", "ano_mes"], "left").fillna(
        0, subset=["qtd_eventos_risco", "qtd_fraude", "qtd_chargeback", "qtd_suspeita"]
    )
    result = result.withColumn(
        "taxa_estorno_pct", F.round(F.col("qtd_estornos") / F.col("qtd_transacoes") * 100, 2)
    )

    staged = result.withColumn("batch_id", F.lit(config.batch_id)).withColumn(
        "timestamp_processamento_gold", F.current_timestamp()
    ).select(*FINAL_COLS)
    staged.persist()
    gravados = staged.count()

    merge_by_composite_key(
        spark, config.table_path("gold", "gold_indicadores_risco"), staged,
        key_cols=["id_cliente", "ano_mes"], final_cols=FINAL_COLS,
    )
    staged.unpersist()

    logger.info("gold_indicadores_risco atualizada", pares_recalculados=gravados)
    return {"tabela": "gold_indicadores_risco", "gravados": gravados}
