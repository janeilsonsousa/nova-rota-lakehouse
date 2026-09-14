# Dimensões Ouro: sempre a versão vigente hoje (não ponto-no-tempo), overwrite total a cada run.

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
    # não tem cadastro de estabelecimento, então deriva por agregação das transações
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
