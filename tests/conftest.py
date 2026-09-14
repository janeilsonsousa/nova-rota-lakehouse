from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pytest

from src.config.settings import PipelineConfig
from src.utils.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark(app_name="nova_rota_tests")
    yield session
    session.stop()


@pytest.fixture()
def tmp_config(tmp_path: Path) -> PipelineConfig:
    base_path = tmp_path / "lakehouse"
    raw_path = tmp_path / "raw"
    checkpoint_path = tmp_path / "checkpoints"
    quarantine_path = base_path / "_quarentena"
    raw_path.mkdir(parents=True, exist_ok=True)
    checkpoint_path.mkdir(parents=True, exist_ok=True)

    return PipelineConfig(
        env="local",
        base_path=str(base_path),
        raw_path=str(raw_path),
        checkpoint_path=str(checkpoint_path),
        quarantine_path=str(quarantine_path),
        batch_id=f"test-{uuid.uuid4().hex[:8]}",
    )


@pytest.fixture()
def cleanup_delta_dirs():
    # limpa data/lakehouse depois de testes que rodam o pipeline com a massa real
    created: list[str] = []
    yield created
    for path in created:
        shutil.rmtree(path, ignore_errors=True)
