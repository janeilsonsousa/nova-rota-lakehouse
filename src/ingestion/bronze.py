"""Camada Bronze: ingestão incremental, idempotente, com preservação do dado
bruto e metadados de rastreabilidade.

Cada fonte é lida arquivo a arquivo (não em lote único) e unida via
``unionByName(allowMissingColumns=True)``: isso é o que permite que um
arquivo com colunas extras (evolução de schema, ex.: ``device_id`` e
``ip_origem`` em ``transacoes_2026-04-10_schema_v2.csv``) conviva com
arquivos antigos sem quebrar a leitura — as colunas ausentes nos arquivos
antigos ficam ``NULL``, e a escrita Delta usa ``mergeSchema=true`` para
evoluir o schema da tabela Bronze automaticamente, preservando o dado bruto
completo (nenhuma coluna inesperada é descartada).
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config.settings import PipelineConfig
from src.ingestion.control import PendingFile, list_pending_files, register_processed_files
from src.utils.logging_utils import get_logger


@dataclass(frozen=True)
class SourceConfig:
    name: str
    glob_pattern: str
    bronze_table: str
    baseline_columns: frozenset[str]


SOURCES: dict[str, SourceConfig] = {
    "clientes": SourceConfig(
        name="clientes",
        glob_pattern="clientes_cdc.csv",
        bronze_table="bronze_clientes_cdc",
        baseline_columns=frozenset(
            {"id_cliente", "cpf", "nome", "cidade", "estado", "renda", "segmento", "data_atualizacao", "operacao"}
        ),
    ),
    "contas": SourceConfig(
        name="contas",
        glob_pattern="contas_cdc.csv",
        bronze_table="bronze_contas_cdc",
        baseline_columns=frozenset(
            {"id_conta", "id_cliente", "tipo_conta", "status_conta", "data_abertura", "data_atualizacao", "operacao"}
        ),
    ),
    "cartoes": SourceConfig(
        name="cartoes",
        glob_pattern="cartoes_cdc.csv",
        bronze_table="bronze_cartoes_cdc",
        baseline_columns=frozenset(
            {"id_cartao", "id_conta", "tipo_cartao", "limite", "status_cartao", "data_atualizacao", "operacao"}
        ),
    ),
    "transacoes": SourceConfig(
        name="transacoes",
        glob_pattern="transacoes/*.csv",
        bronze_table="bronze_transacoes",
        baseline_columns=frozenset(
            {"id_transacao", "id_cartao", "data_transacao", "valor", "mcc", "estabelecimento", "canal", "pais", "moeda"}
        ),
    ),
    "eventos_risco": SourceConfig(
        name="eventos_risco",
        glob_pattern="eventos_risco.csv",
        bronze_table="bronze_eventos_risco",
        baseline_columns=frozenset({"id_evento", "id_transacao", "tipo_evento", "severidade", "data_evento"}),
    ),
    "estornos": SourceConfig(
        name="estornos",
        glob_pattern="estornos.csv",
        bronze_table="bronze_estornos",
        baseline_columns=frozenset({"id_estorno", "id_transacao", "data_estorno", "motivo"}),
    ),
}


def _read_csv_with_origin(spark: SparkSession, pending: PendingFile) -> DataFrame:
    df = spark.read.option("header", True).option("inferSchema", True).csv(pending.path)
    return df.withColumn("_arquivo_origem", F.lit(pending.arquivo_origem))


def _read_pending_files(spark: SparkSession, files: list[PendingFile]) -> DataFrame:
    dfs = [_read_csv_with_origin(spark, f) for f in files]
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


def _add_bronze_metadata(df: DataFrame, config: PipelineConfig, source: SourceConfig) -> DataFrame:
    original_cols = [c for c in df.columns if c != "_arquivo_origem"]
    df = df.withColumn(
        "hash_linha",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in sorted(original_cols)]), 256),
    )
    extra_cols = set(original_cols) - set(source.baseline_columns)
    df = df.withColumn("schema_version", F.lit(2 if extra_cols else 1))
    df = (
        df.withColumnRenamed("_arquivo_origem", "arquivo_origem")
        .withColumn("data_ingestao", F.current_date())
        .withColumn("timestamp_ingestao", F.current_timestamp())
        .withColumn("batch_id", F.lit(config.batch_id))
    )
    return df


def ingest_source(spark: SparkSession, config: PipelineConfig, source_key: str) -> dict:
    """Ingesta incrementalmente uma fonte. Retorna métricas da execução.

    Idempotente: reexecutar sem novos arquivos não escreve nada (0 linhas).
    Reprocessamento total: ``config.run_mode == 'full'`` ignora o controle e
    relê tudo (útil para backfill/correção retroativa).
    """
    logger = get_logger("ingestion.bronze", batch_id=config.batch_id).bind(source=source_key)
    source = SOURCES[source_key]

    pending = list_pending_files(spark, config, source.name, config.raw_path, source.glob_pattern)
    if not pending:
        logger.info("nenhum arquivo novo para ingerir", tabela=source.bronze_table)
        return {"source": source_key, "arquivos_processados": 0, "linhas_ingeridas": 0}

    logger.info("arquivos pendentes encontrados", qtd=len(pending), arquivos=[p.arquivo_origem for p in pending])

    raw_df = _read_pending_files(spark, pending)
    bronze_df = _add_bronze_metadata(raw_df, config, source)

    target_path = config.table_path("bronze", source.bronze_table)
    # bronze_df é lido/computado uma única vez (persist) e reaproveitado
    # para o count() e o write(): sem isso, Spark recalcularia o plano do
    # zero para cada ação (2 jobs), o que além de custar caro também expôs
    # instabilidade do worker Python em execução local no Windows.
    bronze_df.persist()
    try:
        row_count = bronze_df.count()
        (
            bronze_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(target_path)
        )
    finally:
        bronze_df.unpersist()

    register_processed_files(spark, config, source.name, pending)
    logger.info("ingestão concluída", tabela=source.bronze_table, linhas=row_count)
    return {"source": source_key, "arquivos_processados": len(pending), "linhas_ingeridas": row_count}


def run_bronze_ingestion(spark: SparkSession, config: PipelineConfig) -> list[dict]:
    logger = get_logger("ingestion.bronze", batch_id=config.batch_id)
    logger.info("iniciando ingestão bronze", run_mode=config.run_mode)
    results = [ingest_source(spark, config, key) for key in SOURCES]
    logger.info("ingestão bronze finalizada", resultados=results)
    return results
