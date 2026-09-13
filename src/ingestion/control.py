"""Controle de ingestão incremental (equivalente funcional ao Auto Loader).

Por que não usamos o Databricks Auto Loader (`cloudFiles`) nesta entrega
--------------------------------------------------------------------------
O Auto Loader é a escolha certa em produção para ingestão incremental de
arquivos em object storage na nuvem (S3/ADLS/GCS): ele usa *directory
listing* incremental ou *file notification* (fila de eventos do storage) e
mantém um checkpoint gerenciado (RocksDB) com o schema inferido e evoluído
automaticamente. Ele não depende de um Volume/Workspace específico, mas sim
de um bucket/container real na nuvem com permissões configuradas.

Nesta entrega os arquivos de origem chegam em um diretório local (ou em um
Volume Unity Catalog quando publicado em Databricks), sem um bucket próprio
para configurar notificações de evento. Reproduzir o Auto Loader "de
verdade" aqui seria: (a) não demonstrável de forma reproduzível por outra
pessoa sem uma conta cloud própria, e (b) desnecessário para o volume de
dados do desafio (poucos arquivos por lote).

Por isso implementamos o mesmo *princípio* de idempotência e incrementalidade
do Auto Loader com uma tabela de controle Delta (`_ingestion_control`):
cada arquivo processado é registrado com um hash de conteúdo; uma nova
execução só processa arquivos novos ou cujo conteúdo mudou (hash diferente).
Isso é o que garante que a ingestão possa ser reexecutada (reprocessamento,
backfill) sem duplicar dados em Bronze — a mesma propriedade de idempotência
que o checkpoint do Auto Loader oferece.

Em produção real na Databricks, a troca é mecânica: substituir
`list_pending_files` + `register_processed_files` por
`spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", checkpoint_path).load(raw_path)` com
`trigger(availableNow=True)` e um `writeStream` para a tabela Bronze — o
restante do pipeline (metadados, escrita Delta) permanece idêntico.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from src.config.settings import PipelineConfig
from src.utils.spark_helpers import build_literal_df

CONTROL_SCHEMA = StructType(
    [
        StructField("source_name", StringType(), False),
        StructField("arquivo_origem", StringType(), False),
        StructField("hash_arquivo", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("timestamp_processamento", TimestampType(), False),
    ]
)


@dataclass(frozen=True)
class PendingFile:
    path: str
    arquivo_origem: str
    hash_arquivo: str


def _control_table_path(config: PipelineConfig) -> str:
    return f"{config.base_path}/{config.bronze_schema}/_ingestion_control"


def _file_hash(path: str) -> str:
    """Hash de conteúdo (sha256) do arquivo — barato para os volumes deste
    desafio; em produção com arquivos grandes, usar (tamanho + mtime) como
    proxy mais barato do que ler o arquivo inteiro.
    """
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_control_table(spark: SparkSession, config: PipelineConfig) -> DeltaTable:
    path = _control_table_path(config)
    if DeltaTable.isDeltaTable(spark, path):
        return DeltaTable.forPath(spark, path)
    empty = build_literal_df(spark, [], CONTROL_SCHEMA)
    empty.write.format("delta").mode("overwrite").save(path)
    return DeltaTable.forPath(spark, path)


def list_pending_files(
    spark: SparkSession,
    config: PipelineConfig,
    source_name: str,
    search_root: str,
    glob_pattern: str = "*.csv",
) -> list[PendingFile]:
    """Lista arquivos novos ou alterados para uma fonte.

    ``run_mode='full'`` ignora o controle e reprocessa tudo (backfill /
    correção retroativa de regra de negócio).
    """
    root = Path(search_root)
    all_files = sorted(str(p) for p in root.rglob(glob_pattern) if p.is_file())

    if config.run_mode == "full":
        return [
            PendingFile(path=f, arquivo_origem=os.path.relpath(f, root), hash_arquivo=_file_hash(f))
            for f in all_files
        ]

    control = ensure_control_table(spark, config)
    processed = (
        control.toDF()
        .filter((F.col("source_name") == source_name) & (F.col("status") == "SUCESSO"))
        .select("arquivo_origem", "hash_arquivo")
        .collect()
    )
    processed_hashes = {(row["arquivo_origem"], row["hash_arquivo"]) for row in processed}

    pending: list[PendingFile] = []
    for f in all_files:
        arquivo_origem = os.path.relpath(f, root)
        file_hash = _file_hash(f)
        if (arquivo_origem, file_hash) not in processed_hashes:
            pending.append(PendingFile(path=f, arquivo_origem=arquivo_origem, hash_arquivo=file_hash))
    return pending


def register_processed_files(
    spark: SparkSession,
    config: PipelineConfig,
    source_name: str,
    files: list[PendingFile],
    status: str = "SUCESSO",
) -> None:
    if not files:
        return
    control_path = _control_table_path(config)
    rows = [
        {
            "source_name": source_name,
            "arquivo_origem": f.arquivo_origem,
            "hash_arquivo": f.hash_arquivo,
            "batch_id": config.batch_id,
            "status": status,
        }
        for f in files
    ]
    static_schema = StructType([field for field in CONTROL_SCHEMA.fields if field.name != "timestamp_processamento"])
    df = build_literal_df(spark, rows, static_schema)
    df = df.withColumn("timestamp_processamento", F.current_timestamp())
    df.write.format("delta").mode("append").save(control_path)


def control_table_df(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    return ensure_control_table(spark, config).toDF()
