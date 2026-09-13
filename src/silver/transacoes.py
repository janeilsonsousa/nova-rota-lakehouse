"""Prata: fato de transações — MERGE incremental idempotente por
``id_transacao``, com tratamento de dados atrasados e reprocessamento por
data de negócio (não por data de ingestão).

Um mesmo ``id_transacao`` pode chegar em lotes diferentes (reenvio,
correção, o arquivo ``*_late.csv`` reenviando transações de janeiro que só
chegaram em abril). O MERGE garante que cada transação apareça exatamente
uma vez na Prata, e a partição de negócio usada nas camadas seguintes é
``dt_transacao`` — extraída de ``data_transacao`` — nunca a data em que o
arquivo chegou. É isso que faz uma transação atrasada "cair" corretamente
no mês de negócio a que pertence, mesmo processada 2-3 meses depois.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.config.settings import PipelineConfig
from src.quality.rules import apply_quality_gate, write_quarantine
from src.silver.common import is_empty, read_bronze_batch
from src.utils.logging_utils import get_logger

VALID_CANAL = {"POS", "ECOMMERCE", "APP", "ATM"}

FINAL_COLS = [
    "id_transacao", "id_cartao", "data_transacao", "dt_transacao", "valor", "mcc",
    "estabelecimento", "canal", "pais", "moeda", "device_id", "ip_origem",
    "schema_version", "arquivo_origem", "batch_id", "timestamp_ingestao",
    "timestamp_processamento_silver",
]


def process_transacoes(spark: SparkSession, config: PipelineConfig) -> dict:
    logger = get_logger("silver.transacoes", batch_id=config.batch_id)
    bronze = read_bronze_batch(spark, config, "bronze_transacoes")
    if is_empty(bronze):
        logger.info("nenhum registro novo para processar")
        return {"entidade": "transacoes", "validos": 0, "quarentena": 0, "gravados": 0}

    cartoes_path = config.table_path("silver", "silver_cartoes")
    if DeltaTable.isDeltaTable(spark, cartoes_path):
        cartoes_validos = spark.read.format("delta").load(cartoes_path).select("id_cartao").distinct()
    else:
        cartoes_validos = spark.createDataFrame([], "id_cartao string")

    bronze_com_flag_cartao = bronze.join(
        cartoes_validos.withColumn("_cartao_existe", F.lit(True)), on="id_cartao", how="left"
    )

    # device_id/ip_origem só existem a partir do arquivo schema_v2; em
    # lotes antigos (schema_version=1) essas colunas não existem no bronze
    # após o unionByName — garantimos que existam aqui (NULL) para o schema
    # final da Prata ser estável independente de quais arquivos já chegaram.
    if "device_id" not in bronze_com_flag_cartao.columns:
        bronze_com_flag_cartao = bronze_com_flag_cartao.withColumn("device_id", F.lit(None).cast("string"))
    if "ip_origem" not in bronze_com_flag_cartao.columns:
        bronze_com_flag_cartao = bronze_com_flag_cartao.withColumn("ip_origem", F.lit(None).cast("string"))

    checks = [
        ("id_transacao_vazio", F.col("id_transacao").isNotNull()),
        ("valor_invalido", F.col("valor") > F.lit(0)),
        ("cartao_inexistente", F.col("_cartao_existe").isNotNull()),
        ("canal_invalido", F.col("canal").isin(list(VALID_CANAL))),
    ]
    gated = apply_quality_gate(bronze_com_flag_cartao, checks)
    write_quarantine(spark, config, gated.quarantined.drop("_cartao_existe"), "transacoes")
    logger.info("quality gate aplicado", validos=gated.valid_count, quarentena=gated.quarantine_count)

    valid = gated.valid.drop("_cartao_existe").withColumn("dt_transacao", F.to_date("data_transacao"))

    # Duplicidade dentro do MESMO lote (ex.: T0000206 repetida 2x no mesmo
    # arquivo mensal): mantém a última linha por hash_linha determinístico.
    window = Window.partitionBy("id_transacao").orderBy(F.col("timestamp_ingestao").desc(), F.col("hash_linha"))
    deduped_in_batch = (
        valid.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
    )

    staged = deduped_in_batch.withColumn(
        "timestamp_processamento_silver", F.current_timestamp()
    ).select(*FINAL_COLS)

    target_path = config.table_path("silver", "silver_transacoes")
    if not DeltaTable.isDeltaTable(spark, target_path):
        staged.write.format("delta").mode("overwrite").save(target_path)
        gravados = staged.count()
    else:
        target = DeltaTable.forPath(spark, target_path)
        gravados = staged.count()
        # MERGE INTO idempotente: uma transação que já existe na Prata (mesmo
        # id_transacao reenviado em outro lote) é atualizada em vez de
        # duplicada; uma transação nova é inserida. Isso é o que garante que
        # reprocessar o mesmo arquivo, ou receber a mesma id_transacao em
        # dois lotes diferentes, nunca infle o valor agregado na Ouro.
        (
            target.alias("t")
            .merge(staged.alias("s"), "t.id_transacao = s.id_transacao")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    logger.info("MERGE concluído", registros_processados=gravados)
    return {
        "entidade": "transacoes",
        "validos": gated.valid_count,
        "quarentena": gated.quarantine_count,
        "gravados": gravados,
    }
