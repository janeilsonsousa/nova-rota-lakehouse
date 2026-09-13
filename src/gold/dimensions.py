"""Ouro: dimensões descritivas (estado atual) para consumo direto por BI.

Diferente de ``gold_fato_transacao`` (que resolve atributos ponto-no-tempo
para preservar o valor histórico correto), estas dimensões trazem sempre a
**versão vigente hoje** — são as tabelas que um analista usa para "quem é
esse cliente" ou "quais estabelecimentos existem", não para reconstruir o
passado. Reescritas por completo a cada execução (``overwrite``): o volume
é pequeno (1 linha por chave de negócio) e a origem (Prata) já garante
idempotência, então não há ganho em fazer MERGE aqui.

Grão e chaves
-------------
- ``gold_dim_cliente``: 1 linha por ``id_cliente`` (apenas a versão com
  ``flag_vigente = true`` na Prata).
- ``gold_dim_conta``: 1 linha por ``id_conta`` (idem).
- ``gold_dim_cartao``: 1 linha por ``id_cartao`` (idem) — cartões cancelados
  aparecem aqui normalmente (com ``status_cartao = 'CANCELADO'``), pois a
  dimensão descreve o estado atual, não filtra por ele; é o consumidor
  (dashboard/query) que decide excluir cancelados de uma métrica.
- ``gold_dim_estabelecimento``: 1 linha por ``(estabelecimento, mcc)`` —
  não existe fonte cadastral de estabelecimentos no domínio, então a
  dimensão é derivada por agregação das transações observadas na Prata.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config.settings import PipelineConfig
from src.silver.scd2 import read_scd2_current


def build_gold_dim_cliente(spark: SparkSession, config: PipelineConfig) -> int:
    df = read_scd2_current(spark, config.table_path("silver", "silver_clientes")).select(
        "id_cliente", "cpf", "nome", "cidade", "estado", "renda", "segmento",
        F.col("dt_inicio_vigencia").alias("cliente_desde"),
    )
    df.write.format("delta").mode("overwrite").save(config.table_path("gold", "gold_dim_cliente"))
    return df.count()


def build_gold_dim_conta(spark: SparkSession, config: PipelineConfig) -> int:
    df = read_scd2_current(spark, config.table_path("silver", "silver_contas")).select(
        "id_conta", "id_cliente", "tipo_conta", "status_conta", "data_abertura",
    )
    df.write.format("delta").mode("overwrite").save(config.table_path("gold", "gold_dim_conta"))
    return df.count()


def build_gold_dim_cartao(spark: SparkSession, config: PipelineConfig) -> int:
    df = read_scd2_current(spark, config.table_path("silver", "silver_cartoes")).select(
        "id_cartao", "id_conta", "tipo_cartao", "limite", "status_cartao",
    )
    df.write.format("delta").mode("overwrite").save(config.table_path("gold", "gold_dim_cartao"))
    return df.count()


def build_gold_dim_estabelecimento(spark: SparkSession, config: PipelineConfig) -> int:
    transacoes = spark.read.format("delta").load(config.table_path("silver", "silver_transacoes"))
    df = transacoes.groupBy("estabelecimento", "mcc").agg(
        F.count("*").alias("qtd_transacoes_historico"),
        F.sum("valor").alias("valor_bruto_historico"),
        F.min("dt_transacao").alias("primeira_transacao"),
        F.max("dt_transacao").alias("ultima_transacao"),
        F.countDistinct("pais").alias("qtd_paises_distintos"),
    )
    df.write.format("delta").mode("overwrite").save(config.table_path("gold", "gold_dim_estabelecimento"))
    return df.count()


def build_gold_dimensions(spark: SparkSession, config: PipelineConfig) -> dict:
    return {
        "gold_dim_cliente": build_gold_dim_cliente(spark, config),
        "gold_dim_conta": build_gold_dim_conta(spark, config),
        "gold_dim_cartao": build_gold_dim_cartao(spark, config),
        "gold_dim_estabelecimento": build_gold_dim_estabelecimento(spark, config),
    }
