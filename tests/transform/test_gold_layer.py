from __future__ import annotations

from pathlib import Path

from src.gold.cliente_mes import build_gold_cliente_mes
from src.gold.fato_transacao import build_gold_fato_transacao
from src.ingestion.bronze import ingest_source
from src.silver.cartoes import process_cartoes
from src.silver.clientes import process_clientes
from src.silver.contas import process_contas
from src.silver.estornos import process_estornos
from src.silver.transacoes import process_transacoes


def _seed_full_chain(spark, config) -> None:
    raw_dir = Path(config.raw_path)
    (raw_dir / "clientes_cdc.csv").write_text(
        "id_cliente,cpf,nome,cidade,estado,renda,segmento,data_atualizacao,operacao\n"
        "C0001,100.000.000-01,Cliente 01,Campinas,SP,1000,Varejo,2026-01-01 00:00:00,I\n"
        "C0001,100.000.000-01,Cliente 01,Sao Paulo,SP,5000,Alta Renda,2026-02-01 00:00:00,U\n",
        encoding="utf-8",
    )
    (raw_dir / "contas_cdc.csv").write_text(
        "id_conta,id_cliente,tipo_conta,status_conta,data_abertura,data_atualizacao,operacao\n"
        "A0001,C0001,PAGAMENTO,ATIVA,2025-01-01,2026-01-01 00:00:00,I\n",
        encoding="utf-8",
    )
    (raw_dir / "cartoes_cdc.csv").write_text(
        "id_cartao,id_conta,tipo_cartao,limite,status_cartao,data_atualizacao,operacao\n"
        "K0001,A0001,CREDITO,1000,ATIVO,2026-01-01 00:00:00,I\n",
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


def test_fato_transacao_resolve_atributos_ponto_no_tempo(spark, tmp_config):
    _seed_full_chain(spark, tmp_config)
    _write_transacoes(tmp_config, "lote1.csv", ["T0001,K0001,2026-01-15 10:00:00,50.00,5411,Loja X,POS,BR,BRL"])
    ingest_source(spark, tmp_config, "transacoes")
    process_transacoes(spark, tmp_config)

    build_gold_fato_transacao(spark, tmp_config)
    fato = spark.read.format("delta").load(tmp_config.table_path("gold", "gold_fato_transacao"))
    row = fato.filter("id_transacao = 'T0001'").collect()[0]

    assert row["cidade_cliente_na_data"] == "Campinas"
    assert row["id_cliente_na_data"] == "C0001"


def test_fato_transacao_estorno_zera_valor_liquido(spark, tmp_config):
    _seed_full_chain(spark, tmp_config)
    _write_transacoes(tmp_config, "lote1.csv", ["T0001,K0001,2026-01-15 10:00:00,50.00,5411,Loja X,POS,BR,BRL"])
    ingest_source(spark, tmp_config, "transacoes")
    process_transacoes(spark, tmp_config)

    raw_dir = Path(tmp_config.raw_path)
    raw_dir.joinpath("estornos.csv").write_text(
        "id_estorno,id_transacao,data_estorno,motivo\nR0001,T0001,2026-01-16 00:00:00,DUPLICIDADE\n",
        encoding="utf-8",
    )
    ingest_source(spark, tmp_config, "estornos")
    process_estornos(spark, tmp_config)

    build_gold_fato_transacao(spark, tmp_config)
    fato = spark.read.format("delta").load(tmp_config.table_path("gold", "gold_fato_transacao"))
    row = fato.filter("id_transacao = 'T0001'").collect()[0]

    assert row["valor"] == 50.00
    assert row["valor_liquido"] == 0.0
    assert row["flag_estornada"] is True


def test_cliente_mes_calcula_variacao_com_lag(spark, tmp_config):
    _seed_full_chain(spark, tmp_config)
    _write_transacoes(
        tmp_config, "lote1.csv",
        [
            "T0001,K0001,2026-01-10 10:00:00,100.00,5411,Loja X,POS,BR,BRL",
            "T0002,K0001,2026-02-10 10:00:00,200.00,5411,Loja X,POS,BR,BRL",
        ],
    )
    ingest_source(spark, tmp_config, "transacoes")
    process_transacoes(spark, tmp_config)
    build_gold_fato_transacao(spark, tmp_config)
    build_gold_cliente_mes(spark, tmp_config)

    cm = spark.read.format("delta").load(tmp_config.table_path("gold", "gold_cliente_mes"))
    rows = {r["ano_mes"]: r for r in cm.filter("id_cliente = 'C0001'").collect()}

    assert rows["2026-01"]["valor_liquido_mes_anterior"] is None
    assert rows["2026-02"]["valor_liquido_mes_anterior"] == 100.00
    assert rows["2026-02"]["variacao_valor_liquido_pct"] == 100.0


def test_gold_pipeline_e_idempotente_em_reexecucao(spark, tmp_config):
    _seed_full_chain(spark, tmp_config)
    _write_transacoes(tmp_config, "lote1.csv", ["T0001,K0001,2026-01-10 10:00:00,100.00,5411,Loja X,POS,BR,BRL"])
    ingest_source(spark, tmp_config, "transacoes")
    process_transacoes(spark, tmp_config)

    build_gold_fato_transacao(spark, tmp_config)
    build_gold_fato_transacao(spark, tmp_config)  # mesmo batch_id, reexecução

    fato = spark.read.format("delta").load(tmp_config.table_path("gold", "gold_fato_transacao"))
    assert fato.count() == 1
