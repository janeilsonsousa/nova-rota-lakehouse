from __future__ import annotations

from datetime import date

import pytest

from src.config.settings import PipelineConfig, get_config


def test_get_config_aplica_defaults_sensatos():
    config = get_config()
    assert config.env == "local"
    assert config.run_mode == "incremental"
    assert config.catalog == "nova_rota"
    assert config.data_referencia == date.today()
    assert config.batch_id  # sempre gerado, nunca vazio


def test_get_config_respeita_overrides_explicitos():
    config = get_config(env="local", run_mode="full", batch_id="meu-lote", catalog="outro_catalogo")
    assert config.run_mode == "full"
    assert config.batch_id == "meu-lote"
    assert config.catalog == "outro_catalogo"


def test_get_config_le_variaveis_de_ambiente(monkeypatch):
    monkeypatch.setenv("NOVAROTA_RUN_MODE", "full")
    monkeypatch.setenv("NOVAROTA_CATALOG", "cat_via_env")
    config = get_config()
    assert config.run_mode == "full"
    assert config.catalog == "cat_via_env"


def test_get_config_kwarg_tem_prioridade_sobre_env_var(monkeypatch):
    monkeypatch.setenv("NOVAROTA_RUN_MODE", "full")
    config = get_config(run_mode="incremental")
    assert config.run_mode == "incremental"


@pytest.mark.parametrize("run_mode", ["invalido", "", "INCREMENTAL"])
def test_pipeline_config_rejeita_run_mode_invalido(run_mode):
    with pytest.raises(ValueError):
        PipelineConfig(run_mode=run_mode)


@pytest.mark.parametrize("env", ["prod", "aws", ""])
def test_pipeline_config_rejeita_env_invalido(env):
    with pytest.raises(ValueError):
        PipelineConfig(env=env)


def test_table_fqn_local_nao_inclui_catalogo():
    config = PipelineConfig(env="local")
    assert config.table_fqn("silver", "silver_clientes") == "silver.silver_clientes"


def test_table_fqn_databricks_inclui_catalogo_unity_catalog():
    config = PipelineConfig(env="databricks", catalog="nova_rota")
    assert config.table_fqn("gold", "gold_fato_transacao") == "nova_rota.gold.gold_fato_transacao"


def test_table_path_monta_caminho_fisico_por_camada():
    config = PipelineConfig(env="local", base_path="/lakehouse")
    assert config.table_path("bronze", "bronze_transacoes") == "/lakehouse/bronze/bronze_transacoes"


def test_table_fqn_camada_invalida_leva_a_erro():
    config = PipelineConfig(env="local")
    with pytest.raises(ValueError):
        config.table_fqn("platina", "algo")
