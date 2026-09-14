from __future__ import annotations

import dataclasses
from pathlib import Path

from src.ingestion.bronze import ingest_source
from src.silver.cartoes import process_cartoes
from src.silver.clientes import process_clientes
from src.silver.contas import process_contas
from src.silver.transacoes import process_transacoes


def _seed_cartao(spark, config, id_cliente="C0001", id_conta="A0001", id_cartao="K0001") -> None:
    raw_dir = Path(config.raw_path)
    (raw_dir / "clientes_cdc.csv").write_text(
        "id_cliente,cpf,nome,cidade,estado,renda,segmento,data_atualizacao,operacao\n"
        f"{id_cliente},100.000.000-01,Cliente 01,Campinas,SP,1000,Varejo,2026-01-01 00:00:00,I\n",
        encoding="utf-8",
    )
    (raw_dir / "contas_cdc.csv").write_text(
        "id_conta,id_cliente,tipo_conta,status_conta,data_abertura,data_atualizacao,operacao\n"
        f"{id_conta},{id_cliente},PAGAMENTO,ATIVA,2025-01-01,2026-01-01 00:00:00,I\n",
        encoding="utf-8",
    )
    (raw_dir / "cartoes_cdc.csv").write_text(
        "id_cartao,id_conta,tipo_cartao,limite,status_cartao,data_atualizacao,operacao\n"
        f"{id_cartao},{id_conta},CREDITO,1000,ATIVO,2026-01-01 00:00:00,I\n",
        encoding="utf-8",
    )
    for source in ("clientes", "contas", "cartoes"):
        ingest_source(spark, config, source)
    process_clientes(spark, config)
    process_contas(spark, config)
    process_cartoes(spark, config)


def _write_transacoes(config, filename: str, rows: list[str]) -> None:
    header = "id_transacao,id_cartao,data_transacao,valor,mcc,estabelecimento,canal,pais,moeda"
    transacoes_dir = Path(config.raw_path) / "transacoes"
    transacoes_dir.mkdir(parents=True, exist_ok=True)
    (transacoes_dir / filename).write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


def test_transacoes_invalidas_vao_para_quarentena(spark, tmp_config):
    _seed_cartao(spark, tmp_config)
    _write_transacoes(
        tmp_config, "lote1.csv",
        [
            "T0001,K0001,2026-01-15 10:00:00,50.00,5411,Loja X,POS,BR,BRL",
            "T0002,K0001,2026-01-15 10:00:00,-10.00,5411,Loja X,POS,BR,BRL",
            "T0003,K9999,2026-01-15 10:00:00,10.00,5411,Loja X,POS,BR,BRL",
        ],
    )
    ingest_source(spark, tmp_config, "transacoes")
    result = process_transacoes(spark, tmp_config)

    assert result["validos"] == 1
    assert result["quarentena"] == 2

    silver = spark.read.format("delta").load(tmp_config.table_path("silver", "silver_transacoes"))
    assert silver.count() == 1
    assert silver.collect()[0]["id_transacao"] == "T0001"


def test_transacoes_merge_evita_duplicidade_entre_lotes(spark, tmp_config):
    _seed_cartao(spark, tmp_config)
    _write_transacoes(tmp_config, "lote1.csv", ["T0001,K0001,2026-01-15 10:00:00,50.00,5411,Loja X,POS,BR,BRL"])
    ingest_source(spark, tmp_config, "transacoes")
    process_transacoes(spark, tmp_config)

    # mesma id_transacao reenviada em um lote posterior (reprocessamento/atraso)
    config2 = dataclasses.replace(tmp_config, batch_id="lote-atrasado")
    _write_transacoes(config2, "lote2_late.csv", ["T0001,K0001,2026-01-15 10:00:00,50.00,5411,Loja X,POS,BR,BRL"])
    ingest_source(spark, config2, "transacoes")
    process_transacoes(spark, config2)

    silver = spark.read.format("delta").load(tmp_config.table_path("silver", "silver_transacoes"))
    assert silver.filter("id_transacao = 'T0001'").count() == 1


def test_transacao_atrasada_cai_na_particao_de_negocio_correta(spark, tmp_config):
    _seed_cartao(spark, tmp_config)
    _write_transacoes(
        tmp_config, "transacoes_2026-04-05_late.csv",
        ["T0099,K0001,2026-01-20 08:00:00,30.00,5411,Loja X,POS,BR,BRL"],
    )
    ingest_source(spark, tmp_config, "transacoes")
    process_transacoes(spark, tmp_config)

    silver = spark.read.format("delta").load(tmp_config.table_path("silver", "silver_transacoes"))
    row = silver.filter("id_transacao = 'T0099'").collect()[0]
    assert str(row["dt_transacao"]) == "2026-01-20"
