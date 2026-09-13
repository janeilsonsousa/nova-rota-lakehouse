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


def get_spark(app_name: str = "nova_rota_lakehouse") -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    _ensure_windows_hadoop_home()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
