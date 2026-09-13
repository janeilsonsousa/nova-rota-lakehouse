"""Ouro: ``gold_features_cliente`` — snapshot de features para Data Science.

Granularidade e chaves
-----------------------
- **Grão**: 1 linha por ``id_cliente`` (snapshot mais recente — não é
  histórico; um modelo de propensão/risco consome "o estado do cliente
  hoje", não uma série temporal completa, que já existe em
  ``gold_cliente_mes``/``gold_indicadores_risco`` para quem precisar).
- **Recomputo**: overwrite completo a cada execução. É um *feature store*
  simplificado — o custo de recalcular tudo é baixo neste volume e a
  alternativa (MERGE incremental) não traria ganho real, já que quase toda
  feature aqui é uma agregação all-time ou uma comparação com o resto da
  base (percentil), que muda para todo mundo a cada nova transação de
  qualquer cliente.
- **RFM + segmentação**: recency/frequency/monetary clássicos, mais
  percentil de gasto dentro da própria cidade e do próprio segmento
  (``PERCENT_RANK``/``NTILE``) — a feature "quanto esse cliente gasta
  comparado a pares parecidos" é normalmente mais informativa para um
  modelo do que o valor absoluto.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.config.settings import PipelineConfig
from src.utils.logging_utils import get_logger


def build_gold_features_cliente(spark: SparkSession, config: PipelineConfig) -> dict:
    logger = get_logger("gold.features_cliente", batch_id=config.batch_id)

    dim_cliente = spark.read.format("delta").load(config.table_path("gold", "gold_dim_cliente"))
    fato = spark.read.format("delta").load(config.table_path("gold", "gold_fato_transacao"))
    cliente_mes = spark.read.format("delta").load(config.table_path("gold", "gold_cliente_mes"))
    indicadores = spark.read.format("delta").load(config.table_path("gold", "gold_indicadores_risco"))

    recencia = fato.groupBy(F.col("id_cliente_na_data").alias("id_cliente")).agg(
        F.max("dt_transacao").alias("ultima_transacao")
    ).withColumn("dias_desde_ultima_transacao", F.datediff(F.current_date(), F.col("ultima_transacao")))

    totais = cliente_mes.groupBy("id_cliente").agg(
        F.sum("qtd_transacoes").alias("qtd_transacoes_total"),
        F.sum("valor_liquido").alias("valor_liquido_total"),
        F.sum("valor_bruto").alias("valor_bruto_total"),
        F.countDistinct("ano_mes").alias("qtd_meses_ativos"),
    )
    totais = totais.withColumn(
        "ticket_medio_total", F.round(F.col("valor_liquido_total") / F.col("qtd_transacoes_total"), 2)
    )

    risco_totais = indicadores.groupBy("id_cliente").agg(
        F.sum("qtd_eventos_risco").alias("qtd_eventos_risco_total"),
        F.sum("qtd_fraude").alias("qtd_fraude_total"),
        F.sum("qtd_chargeback").alias("qtd_chargeback_total"),
        F.sum("qtd_estornos").alias("qtd_estornos_total"),
    )

    base = (
        dim_cliente.select("id_cliente", "segmento", "cidade", "estado", "renda", "cliente_desde")
        .withColumn("dias_desde_cadastro", F.datediff(F.current_date(), F.col("cliente_desde")))
        .join(recencia.select("id_cliente", "dias_desde_ultima_transacao"), "id_cliente", "left")
        .join(totais, "id_cliente", "left")
        .join(risco_totais, "id_cliente", "left")
        .fillna(
            0,
            subset=[
                "qtd_transacoes_total", "valor_liquido_total", "valor_bruto_total", "qtd_meses_ativos",
                "qtd_eventos_risco_total", "qtd_fraude_total", "qtd_chargeback_total", "qtd_estornos_total",
            ],
        )
    )

    # Segmentação: percentil de gasto dentro da própria cidade e do próprio
    # segmento de negócio, e decil de gasto geral — usa PERCENT_RANK/NTILE.
    win_cidade = Window.partitionBy("cidade").orderBy(F.col("valor_liquido_total"))
    win_segmento = Window.partitionBy("segmento").orderBy(F.col("valor_liquido_total"))
    win_geral = Window.orderBy(F.col("valor_liquido_total"))

    result = (
        base.withColumn("percentil_gasto_cidade", F.round(F.percent_rank().over(win_cidade), 4))
        .withColumn("percentil_gasto_segmento", F.round(F.percent_rank().over(win_segmento), 4))
        .withColumn("decil_gasto_geral", F.ntile(10).over(win_geral))
        .withColumn("timestamp_processamento_gold", F.current_timestamp())
    )

    result.write.format("delta").mode("overwrite").save(config.table_path("gold", "gold_features_cliente"))
    gravados = result.count()
    logger.info("gold_features_cliente atualizada", registros=gravados)
    return {"tabela": "gold_features_cliente", "gravados": gravados}
