"""Prata: dimensão de clientes com histórico SCD Tipo 2."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config.settings import PipelineConfig
from src.quality.rules import CPF_FORMAT_REGEX, VALID_UF, apply_quality_gate, dedupe_exact_duplicates, write_quarantine
from src.silver.common import is_empty, read_bronze_batch
from src.silver.scd2 import apply_scd2
from src.utils.logging_utils import get_logger

ATTRIBUTE_COLS = ["cpf", "nome", "cidade", "estado", "renda", "segmento"]


def process_clientes(spark: SparkSession, config: PipelineConfig) -> dict:
    logger = get_logger("silver.clientes", batch_id=config.batch_id)
    bronze = read_bronze_batch(spark, config, "bronze_clientes_cdc")
    if is_empty(bronze):
        logger.info("nenhum registro novo para processar")
        return {"entidade": "clientes", "validos": 0, "quarentena": 0, "chaves_afetadas": 0}

    checks = [
        ("estado_invalido", F.col("estado").isin(list(VALID_UF))),
        ("renda_negativa", F.col("renda") >= F.lit(0)),
        ("cpf_formato_invalido", F.col("cpf").rlike(CPF_FORMAT_REGEX)),
        ("nome_vazio", F.col("nome").isNotNull() & (F.length(F.trim(F.col("nome"))) > 0)),
        ("id_cliente_vazio", F.col("id_cliente").isNotNull()),
    ]
    gated = apply_quality_gate(bronze, checks)
    write_quarantine(spark, config, gated.quarantined, "clientes")
    logger.info("quality gate aplicado", validos=gated.valid_count, quarentena=gated.quarantine_count)

    # Regra cross-row: mesmo CPF em id_cliente diferentes é um problema de
    # cadastro (duplicidade de identidade) — não dá para decidir qual dos
    # dois é o "correto" automaticamente, então os dois vão para quarentena
    # para curadoria manual em vez de silenciosamente escolher um.
    valid = gated.valid
    cpf_owners = valid.groupBy("cpf").agg(F.countDistinct("id_cliente").alias("qtd_clientes_distintos"))
    cpf_duplicado = cpf_owners.filter(F.col("qtd_clientes_distintos") > 1).select("cpf")
    linhas_cpf_duplicado = valid.join(cpf_duplicado, "cpf", "inner").withColumn(
        "motivos_quarentena", F.lit("cpf_duplicado_entre_clientes_distintos")
    )
    write_quarantine(spark, config, linhas_cpf_duplicado, "clientes")
    valid = valid.join(cpf_duplicado, "cpf", "left_anti")

    # Duplicata exata (mesmo id_cliente + mesma data_atualizacao, ex.: C0005
    # com 2 linhas idênticas no mesmo lote) — mantém 1 registro determinístico.
    deduped = dedupe_exact_duplicates(valid, ["id_cliente"], "data_atualizacao")

    result = apply_scd2(
        spark, config,
        config.table_path("silver", "silver_clientes"),
        deduped,
        business_key="id_cliente",
        attribute_cols=ATTRIBUTE_COLS,
    )
    logger.info("SCD2 aplicado", chaves_afetadas=result.chaves_afetadas, versoes_gravadas=result.versoes_gravadas)
    return {
        "entidade": "clientes",
        "validos": gated.valid_count,
        "quarentena": gated.quarantine_count,
        "chaves_afetadas": result.chaves_afetadas,
    }
