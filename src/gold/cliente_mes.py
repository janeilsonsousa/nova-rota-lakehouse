# gold_cliente_mes: métricas mensais por cliente, grão (id_cliente, ano_mes), sempre lendo
# de gold_fato_transacao. Recalcula o (cliente, mês) inteiro em vez de somar incremental,
# porque ticket médio e outras métricas não são aditivas.

from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.config.settings import PipelineConfig
from src.gold.common import merge_by_composite_key
from src.silver.common import is_empty
from src.utils.logging_utils import get_logger

FINAL_COLS = [
    "id_cliente", "ano_mes",
    "qtd_transacoes", "valor_bruto", "valor_liquido", "valor_estornado", "ticket_medio",
    "qtd_estabelecimentos_distintos", "qtd_paises_distintos", "qtd_transacoes_internacionais",
    "qtd_estornos", "qtd_eventos_risco",
    "valor_liquido_mes_anterior", "variacao_valor_liquido_pct",
    "batch_id", "timestamp_processamento_gold",
]


def build_gold_cliente_mes(spark: SparkSession, config: PipelineConfig) -> dict:
    logger = get_logger("gold.cliente_mes", batch_id=config.batch_id)
    fato_path = config.table_path("gold", "gold_fato_transacao")
    fato = spark.read.format("delta").load(fato_path)

    affected = (
        fato.filter(F.col("batch_id") == config.batch_id)
        .select("id_cliente_na_data", F.date_format("dt_transacao", "yyyy-MM").alias("ano_mes"))
        .distinct()
    ) if config.run_mode != "full" else None

    fato_com_mes = fato.withColumn("ano_mes", F.date_format(F.col("dt_transacao"), "yyyy-MM")).withColumn(
        "id_cliente", F.col("id_cliente_na_data")
    )
    if affected is not None:
        if is_empty(affected):
            logger.info("nenhum (cliente, mês) afetado nesta execução")
            return {"tabela": "gold_cliente_mes", "gravados": 0}
        # reagrega o mês inteiro, não só as linhas novas
        pares_afetados = affected.withColumnRenamed("id_cliente_na_data", "id_cliente")
        fato_escopo = fato_com_mes.join(pares_afetados, ["id_cliente", "ano_mes"], "inner")
    else:
        fato_escopo = fato_com_mes

    agg = fato_escopo.groupBy("id_cliente", "ano_mes").agg(
        F.count("*").alias("qtd_transacoes"),
        F.sum("valor").alias("valor_bruto"),
        F.sum("valor_liquido").alias("valor_liquido"),
        F.countDistinct("estabelecimento").alias("qtd_estabelecimentos_distintos"),
        F.countDistinct("pais").alias("qtd_paises_distintos"),
        F.sum(F.when(F.col("pais") != F.lit("BR"), 1).otherwise(0)).alias("qtd_transacoes_internacionais"),
        F.sum(F.when(F.col("flag_estornada"), 1).otherwise(0)).alias("qtd_estornos"),
        F.sum(F.when(F.col("flag_evento_risco"), 1).otherwise(0)).alias("qtd_eventos_risco"),
    )
    agg = (
        agg.withColumn("valor_estornado", F.col("valor_bruto") - F.col("valor_liquido"))
        .withColumn("ticket_medio", F.round(F.col("valor_liquido") / F.col("qtd_transacoes"), 2))
    )

    # LAG precisa do histórico completo, não só dos meses afetados
    historico = fato_com_mes.groupBy("id_cliente", "ano_mes").agg(F.sum("valor_liquido").alias("valor_liquido"))
    window = Window.partitionBy("id_cliente").orderBy("ano_mes")
    historico = historico.withColumn("valor_liquido_mes_anterior", F.lag("valor_liquido").over(window)).select(
        "id_cliente", "ano_mes", "valor_liquido_mes_anterior"
    )

    result = agg.join(historico, ["id_cliente", "ano_mes"], "left").withColumn(
        "variacao_valor_liquido_pct",
        F.when(
            (F.col("valor_liquido_mes_anterior").isNotNull()) & (F.col("valor_liquido_mes_anterior") != 0),
            F.round(
                (F.col("valor_liquido") - F.col("valor_liquido_mes_anterior"))
                / F.col("valor_liquido_mes_anterior")
                * 100,
                2,
            ),
        ),
    )

    staged = result.withColumn("batch_id", F.lit(config.batch_id)).withColumn(
        "timestamp_processamento_gold", F.current_timestamp()
    ).select(*FINAL_COLS)
    gravados = staged.count()

    target_path = config.table_path("gold", "gold_cliente_mes")
    merge_by_composite_key(spark, target_path, staged, key_cols=["id_cliente", "ano_mes"], final_cols=FINAL_COLS)

    logger.info("gold_cliente_mes atualizada", pares_recalculados=gravados)
    return {"tabela": "gold_cliente_mes", "gravados": gravados}
