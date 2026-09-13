"""Ouro: ``gold_fato_transacao`` — grão de 1 linha por ``id_transacao``.

Granularidade, chaves e regras (ver também docs/data_contracts.md)
--------------------------------------------------------------------
- **Grão**: 1 linha = 1 transação (``id_transacao``), já deduplicada pela
  Prata (MERGE incremental) — nunca há mais de uma linha por id_transacao.
- **Chaves**: ``id_transacao`` (PK), ``id_cartao``, ``id_conta_na_data``,
  ``id_cliente_na_data`` — as duas últimas resolvidas por join ponto-no-tempo
  (não é a conta/cliente atual do cartão, é quem era o dono na data da
  transação).
- **Filtros de negócio aplicados**: nenhum registro é excluído aqui — até
  transações de cartão cancelado aparecem (com
  ``flag_cartao_cancelado_no_momento``), porque o cancelamento pode ter
  acontecido DEPOIS da transação. A exclusão de cartões cancelados de
  métricas "futuras" acontece nos agregados (gold_cliente_mes), filtrando
  pela vigência atual do cartão, não aqui na granularidade transacional.
- **valor_liquido**: igual a ``valor`` quando não há estorno; 0 quando há
  pelo menos 1 estorno vinculado (premissa documentada em
  src/silver/estornos.py, já que o layout não traz valor parcial de
  estorno).
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config.settings import PipelineConfig
from src.gold.common import join_ponto_no_tempo, read_silver_or_empty, severidade_rank_col
from src.silver.common import is_empty
from src.utils.logging_utils import get_logger

FINAL_COLS = [
    "id_transacao", "id_cartao", "id_conta_na_data", "id_cliente_na_data",
    "data_transacao", "dt_transacao", "valor", "valor_liquido",
    "flag_estornada", "qtd_estornos",
    "status_cartao_na_data", "flag_cartao_cancelado_no_momento",
    "segmento_cliente_na_data", "cidade_cliente_na_data", "estado_cliente_na_data",
    "mcc", "estabelecimento", "canal", "pais", "moeda", "device_id", "ip_origem",
    "flag_evento_risco", "qtd_eventos_risco", "severidade_maxima_risco",
    "batch_id", "timestamp_processamento_gold",
]


def build_gold_fato_transacao(spark: SparkSession, config: PipelineConfig) -> dict:
    logger = get_logger("gold.fato_transacao", batch_id=config.batch_id)

    transacoes = spark.read.format("delta").load(config.table_path("silver", "silver_transacoes"))
    if config.run_mode != "full":
        transacoes = transacoes.filter(F.col("batch_id") == config.batch_id)
    if is_empty(transacoes):
        logger.info("nenhuma transação nova para consolidar na Ouro")
        return {"tabela": "gold_fato_transacao", "gravados": 0}

    cartoes = spark.read.format("delta").load(config.table_path("silver", "silver_cartoes"))
    contas = spark.read.format("delta").load(config.table_path("silver", "silver_contas"))
    clientes = spark.read.format("delta").load(config.table_path("silver", "silver_clientes"))
    estornos = read_silver_or_empty(spark, config.table_path("silver", "silver_estornos"), ["id_transacao"])
    eventos = read_silver_or_empty(
        spark, config.table_path("silver", "silver_eventos_risco"), ["id_transacao", "severidade", "tipo_evento"]
    )

    # Cada join ponto-no-tempo usa o nome de chave NATURAL da dimensão do
    # lado direito (join_ponto_no_tempo busca essa coluna dentro da própria
    # dimensão) — por isso renomeamos a FK recém-trazida para o nome que a
    # próxima dimensão espera antes de encadear o próximo join.
    with_cartao = join_ponto_no_tempo(transacoes, cartoes, "id_cartao", "data_transacao", "cartao_")

    with_cartao = with_cartao.withColumnRenamed("cartao_id_conta", "id_conta")
    with_conta = join_ponto_no_tempo(with_cartao, contas, "id_conta", "data_transacao", "conta_")

    with_conta = with_conta.withColumnRenamed("conta_id_cliente", "id_cliente")
    with_cliente = join_ponto_no_tempo(with_conta, clientes, "id_cliente", "data_transacao", "cliente_")

    with_cliente = with_cliente.withColumnRenamed("id_conta", "id_conta_na_data").withColumnRenamed(
        "id_cliente", "id_cliente_na_data"
    )

    estornos_agg = estornos.groupBy("id_transacao").agg(F.count("*").alias("qtd_estornos"))
    eventos_agg = eventos.groupBy("id_transacao").agg(
        F.count("*").alias("qtd_eventos_risco"),
        F.max(severidade_rank_col("severidade")).alias("_rank_max"),
    )
    rank_to_label = F.create_map(*[F.lit(x) for pair in {1: "BAIXA", 2: "MEDIA", 3: "ALTA", 4: "CRITICA"}.items() for x in pair])
    eventos_agg = eventos_agg.withColumn("severidade_maxima_risco", rank_to_label[F.col("_rank_max")]).drop("_rank_max")

    enriched = (
        with_cliente.join(estornos_agg, "id_transacao", "left")
        .join(eventos_agg, "id_transacao", "left")
        .withColumn("qtd_estornos", F.coalesce(F.col("qtd_estornos"), F.lit(0)))
        .withColumn("qtd_eventos_risco", F.coalesce(F.col("qtd_eventos_risco"), F.lit(0)))
        .withColumn("flag_estornada", F.col("qtd_estornos") > 0)
        .withColumn("flag_evento_risco", F.col("qtd_eventos_risco") > 0)
        .withColumn("valor_liquido", F.when(F.col("flag_estornada"), F.lit(0.0)).otherwise(F.col("valor")))
        .withColumnRenamed("cartao_status_cartao", "status_cartao_na_data")
        .withColumn("flag_cartao_cancelado_no_momento", F.col("status_cartao_na_data") == F.lit("CANCELADO"))
        .withColumnRenamed("cliente_segmento", "segmento_cliente_na_data")
        .withColumnRenamed("cliente_cidade", "cidade_cliente_na_data")
        .withColumnRenamed("cliente_estado", "estado_cliente_na_data")
        .withColumn("timestamp_processamento_gold", F.current_timestamp())
    )

    staged = enriched.select(*FINAL_COLS)
    target_path = config.table_path("gold", "gold_fato_transacao")

    gravados = staged.count()
    if not DeltaTable.isDeltaTable(spark, target_path):
        staged.write.format("delta").mode("overwrite").save(target_path)
    else:
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("t")
            .merge(staged.alias("s"), "t.id_transacao = s.id_transacao")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    logger.info("gold_fato_transacao atualizada", registros=gravados)
    return {"tabela": "gold_fato_transacao", "gravados": gravados}
