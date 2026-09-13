from __future__ import annotations

import dataclasses
from pathlib import Path

from src.ingestion.bronze import ingest_source
from src.silver.clientes import process_clientes


def _write_clientes_csv(path: Path, rows: list[str]) -> None:
    header = "id_cliente,cpf,nome,cidade,estado,renda,segmento,data_atualizacao,operacao"
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


def test_clientes_invalidos_vao_para_quarentena_e_nao_para_silver(spark, tmp_config):
    _write_clientes_csv(
        Path(tmp_config.raw_path) / "clientes_cdc.csv",
        [
            "C0001,100.000.000-01,Cliente Bom,Campinas,SP,1000,Varejo,2026-01-01 00:00:00,I",
            "C0002,100.000.000-02,Cliente Estado Invalido,Campinas,XX,1000,Varejo,2026-01-01 00:00:00,I",
            "C0003,100.000.000-03,Cliente Renda Negativa,Campinas,SP,-500,Varejo,2026-01-01 00:00:00,I",
        ],
    )
    ingest_source(spark, tmp_config, "clientes")
    result = process_clientes(spark, tmp_config)

    assert result["validos"] == 1
    assert result["quarentena"] == 2

    silver = spark.read.format("delta").load(tmp_config.table_path("silver", "silver_clientes"))
    assert silver.count() == 1
    assert silver.collect()[0]["id_cliente"] == "C0001"

    quarentena = spark.read.format("delta").load(f"{tmp_config.quarantine_path}/clientes")
    assert quarentena.count() == 2


def test_clientes_scd2_cria_nova_versao_quando_atributo_muda(spark, tmp_config):
    raw_dir = Path(tmp_config.raw_path)
    _write_clientes_csv(
        raw_dir / "clientes_cdc.csv",
        ["C0001,100.000.000-01,Cliente 01,Campinas,SP,1000,Varejo,2026-01-01 00:00:00,I"],
    )
    ingest_source(spark, tmp_config, "clientes")
    process_clientes(spark, tmp_config)

    # segundo lote: cliente muda de cidade e renda (simula uma segunda
    # execução do pipeline, com um novo batch_id)
    tmp_config2 = dataclasses.replace(tmp_config, batch_id="segundo-lote")
    _write_clientes_csv(
        raw_dir / "clientes_cdc.csv",
        [
            "C0001,100.000.000-01,Cliente 01,Campinas,SP,1000,Varejo,2026-01-01 00:00:00,I",
            "C0001,100.000.000-01,Cliente 01,Sao Paulo,SP,5000,Alta Renda,2026-02-01 00:00:00,U",
        ],
    )
    ingest_source(spark, tmp_config2, "clientes")
    process_clientes(spark, tmp_config2)

    silver = spark.read.format("delta").load(tmp_config.table_path("silver", "silver_clientes"))
    rows = silver.orderBy("versao").collect()
    assert len(rows) == 2
    assert rows[0]["cidade"] == "Campinas" and rows[0]["flag_vigente"] is False
    assert rows[1]["cidade"] == "Sao Paulo" and rows[1]["flag_vigente"] is True
    assert rows[0]["dt_fim_vigencia"] is not None


def test_clientes_reexecucao_do_mesmo_lote_e_idempotente(spark, tmp_config):
    _write_clientes_csv(
        Path(tmp_config.raw_path) / "clientes_cdc.csv",
        ["C0001,100.000.000-01,Cliente 01,Campinas,SP,1000,Varejo,2026-01-01 00:00:00,I"],
    )
    ingest_source(spark, tmp_config, "clientes")
    process_clientes(spark, tmp_config)
    process_clientes(spark, tmp_config)  # nada de novo no bronze para este batch_id

    silver = spark.read.format("delta").load(tmp_config.table_path("silver", "silver_clientes"))
    assert silver.count() == 1
