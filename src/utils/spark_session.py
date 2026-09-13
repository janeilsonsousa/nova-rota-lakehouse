"""Fábrica de SparkSession portátil entre Databricks e execução local.

Em um notebook Databricks já existe uma ``SparkSession`` ativa (variável
global ``spark``) com o Delta Lake e o Unity Catalog configurados pelo
runtime — nesse caso apenas reaproveitamos essa sessão. Em execução local
(testes, desenvolvimento, CI) construímos uma sessão Spark local com o
Delta Lake OSS via ``delta-spark``, para que o mesmo código de
ingestion/silver/gold rode sem nenhuma dependência de cluster.
"""

from __future__ import annotations

import os
import platform
from pathlib import Path

from pyspark.sql import SparkSession


def _ensure_windows_hadoop_home() -> None:
    """No Windows, o Spark local precisa do winutils.exe (HADOOP_HOME) para
    operações de filesystem usadas pelo Delta Lake. Em Databricks isso nunca
    é necessário (roda em Linux); aqui detectamos automaticamente o
    ``winutils`` versionado no próprio repositório (``tools/hadoop-win``)
    para que qualquer pessoa no Windows rode o projeto sem setup manual.
    """
    if platform.system() != "Windows" or os.environ.get("HADOOP_HOME"):
        return
    candidate = Path(__file__).resolve().parents[2] / "tools" / "hadoop-win"
    if (candidate / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(candidate)
        os.environ["PATH"] = str(candidate / "bin") + os.pathsep + os.environ.get("PATH", "")


def _ensure_worker_python() -> None:
    """Garante que os workers do PySpark usem o mesmo interpretador do
    driver (o Python do venv), em vez de resolver ``python`` pelo PATH do
    SO — no Windows isso pode acidentalmente pegar o alias da Microsoft
    Store (WindowsApps) e quebrar a serialização com um erro de socket
    difícil de diagnosticar (``Connection reset``).
    """
    import sys

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark(app_name: str = "nova_rota_lakehouse") -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    _ensure_windows_hadoop_home()
    _ensure_worker_python()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        # local[1]: no PySpark 3.5.x + Python 3.12 no Windows, partições
        # vazias derrubam o worker Python com um EOFException pouco
        # descritivo (bug de ambiente, não do pipeline — ver docs/decisions.md,
        # ADR "Execução local no Windows"). Com 1 core e minPartitionNum=1
        # eliminamos partições vazias no dev local; em Databricks (cluster
        # real, Linux) essa restrição não existe e o paralelismo é normal.
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.files.minPartitionNum", "1")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
