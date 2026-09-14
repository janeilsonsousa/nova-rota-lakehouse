# Cria a SparkSession. No Databricks reaproveita a sessão ativa; local, monta uma com Delta OSS.

from __future__ import annotations

import os
import platform
from pathlib import Path

from pyspark.sql import SparkSession


def _ensure_windows_hadoop_home() -> None:
    # winutils.exe vem versionado em tools/hadoop-win pra não precisar instalar nada
    if platform.system() != "Windows" or os.environ.get("HADOOP_HOME"):
        return
    candidate = Path(__file__).resolve().parents[2] / "tools" / "hadoop-win"
    if (candidate / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(candidate)
        os.environ["PATH"] = str(candidate / "bin") + os.pathsep + os.environ.get("PATH", "")


def _ensure_worker_python() -> None:
    # sem isso o Windows às vezes resolve "python" pro alias da Store e quebra o socket
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
        # local[1] + minPartitionNum 1: evita partição vazia, que derruba o worker no Windows
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
        .config("spark.python.worker.reuse", "false")  # worker reciclado dá pau em sessão longa
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
