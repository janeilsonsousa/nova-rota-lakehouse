from __future__ import annotations

from pathlib import Path

from src.ingestion.bronze import ingest_source


def _write_csv(path: Path, header: str, rows: list[str]) -> None:
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


def test_ingest_source_carrega_linhas_e_metadados(spark, tmp_config):
    raw_dir = Path(tmp_config.raw_path)
    _write_csv(
        raw_dir / "clientes_cdc.csv",
        "id_cliente,cpf,nome,cidade,estado,renda,segmento,data_atualizacao,operacao",
        [
            "C0001,100.000.000-01,Cliente 01,Campinas,SP,1000,Varejo,2026-01-01 00:00:00,I",
            "C0002,100.000.000-02,Cliente 02,Campinas,SP,2000,Varejo,2026-01-02 00:00:00,I",
        ],
    )

    result = ingest_source(spark, tmp_config, "clientes")

    assert result == {"source": "clientes", "arquivos_processados": 1, "linhas_ingeridas": 2}

    df = spark.read.format("delta").load(tmp_config.table_path("bronze", "bronze_clientes_cdc"))
    assert df.count() == 2
    expected_meta_cols = {
        "arquivo_origem", "data_ingestao", "timestamp_ingestao", "batch_id", "hash_linha", "schema_version",
    }
    assert expected_meta_cols.issubset(set(df.columns))
    assert {r["batch_id"] for r in df.select("batch_id").collect()} == {tmp_config.batch_id}
    assert {r["schema_version"] for r in df.select("schema_version").collect()} == {1}


def test_ingest_source_e_idempotente_em_reexecucao(spark, tmp_config):
    raw_dir = Path(tmp_config.raw_path)
    _write_csv(
        raw_dir / "estornos.csv",
        "id_estorno,id_transacao,data_estorno,motivo",
        ["R0001,T0001,2026-01-01 00:00:00,DUPLICIDADE"],
    )

    first = ingest_source(spark, tmp_config, "estornos")
    second = ingest_source(spark, tmp_config, "estornos")

    assert first["linhas_ingeridas"] == 1
    assert second == {"source": "estornos", "arquivos_processados": 0, "linhas_ingeridas": 0}

    df = spark.read.format("delta").load(tmp_config.table_path("bronze", "bronze_estornos"))
    assert df.count() == 1  # não duplicou ao reexecutar


def test_ingest_source_preserva_evolucao_de_schema(spark, tmp_config):
    raw_dir = Path(tmp_config.raw_path)
    transacoes_dir = raw_dir / "transacoes"
    transacoes_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        transacoes_dir / "transacoes_2026-01-31.csv",
        "id_transacao,id_cartao,data_transacao,valor,mcc,estabelecimento,canal,pais,moeda",
        ["T0001,K001,2026-01-15 10:00:00,50.00,5411,Loja X,POS,BR,BRL"],
    )
    ingest_source(spark, tmp_config, "transacoes")

    _write_csv(
        transacoes_dir / "transacoes_2026-04-10_schema_v2.csv",
        "id_transacao,id_cartao,data_transacao,valor,mcc,estabelecimento,canal,pais,moeda,device_id,ip_origem",
        ["T0002,K002,2026-04-10 09:00:00,75.00,5732,Loja Y,APP,BR,BRL,DEV-1,10.0.0.1"],
    )
    result = ingest_source(spark, tmp_config, "transacoes")
    assert result["linhas_ingeridas"] == 1

    df = spark.read.format("delta").load(tmp_config.table_path("bronze", "bronze_transacoes"))
    assert df.count() == 2
    assert "device_id" in df.columns and "ip_origem" in df.columns

    versions = {r["id_transacao"]: r["schema_version"] for r in df.select("id_transacao", "schema_version").collect()}
    assert versions == {"T0001": 1, "T0002": 2}

    linha_antiga = df.filter("id_transacao = 'T0001'").collect()[0]
    assert linha_antiga["device_id"] is None


def test_ingest_source_preserva_duplicidade_bruta_entre_arquivos(spark, tmp_config):
    # bronze não deduplica, isso é trabalho da silver
    raw_dir = Path(tmp_config.raw_path)
    transacoes_dir = raw_dir / "transacoes"
    transacoes_dir.mkdir(parents=True, exist_ok=True)

    header = "id_transacao,id_cartao,data_transacao,valor,mcc,estabelecimento,canal,pais,moeda"
    _write_csv(transacoes_dir / "lote1.csv", header, ["T0001,K001,2026-01-15 10:00:00,50.00,5411,Loja X,POS,BR,BRL"])
    _write_csv(transacoes_dir / "lote2.csv", header, ["T0001,K001,2026-01-15 10:00:00,50.00,5411,Loja X,POS,BR,BRL"])

    ingest_source(spark, tmp_config, "transacoes")

    df = spark.read.format("delta").load(tmp_config.table_path("bronze", "bronze_transacoes"))
    assert df.filter("id_transacao = 'T0001'").count() == 2
