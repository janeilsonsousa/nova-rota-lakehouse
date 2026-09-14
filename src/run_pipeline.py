# Entry point do pipeline: Bronze -> Prata -> Ouro.
# python -m src.run_pipeline --env local --run-mode incremental
# python -m src.run_pipeline --env databricks --catalog nova_rota --data-referencia 2026-04-10

from __future__ import annotations

import argparse
import sys
from datetime import datetime

from src.config.settings import get_config
from src.gold.run_gold import run_gold_processing
from src.ingestion.bronze import run_bronze_ingestion
from src.silver.run_silver import run_silver_processing
from src.utils.logging_utils import get_logger
from src.utils.spark_session import get_spark


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline NovaRota Lakehouse (Bronze -> Prata -> Ouro)")
    parser.add_argument("--env", choices=["local", "databricks"], default="local")
    parser.add_argument("--catalog", default="nova_rota", help="Catálogo Unity Catalog (ignorado em --env local)")
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument("--silver-schema", default="silver")
    parser.add_argument("--gold-schema", default="gold")
    parser.add_argument("--base-path", default=None, help="Raiz de armazenamento Delta (default: data/lakehouse)")
    parser.add_argument("--raw-path", default=None, help="Diretório de origem dos arquivos CDC/CSV")
    parser.add_argument("--checkpoint-path", default=None)
    parser.add_argument("--quarantine-path", default=None)
    parser.add_argument(
        "--data-referencia", default=None,
        help="Data de referência do processamento, formato YYYY-MM-DD (default: hoje)",
    )
    parser.add_argument("--run-mode", choices=["incremental", "full"], default="incremental")
    parser.add_argument("--batch-id", default=None, help="ID do lote (default: gerado automaticamente)")
    parser.add_argument(
        "--layers", default="bronze,silver,gold",
        help="Camadas a executar, separadas por vírgula (ex.: 'bronze,silver')",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    overrides = {
        "env": args.env,
        "catalog": args.catalog,
        "bronze_schema": args.bronze_schema,
        "silver_schema": args.silver_schema,
        "gold_schema": args.gold_schema,
        "run_mode": args.run_mode,
    }
    if args.base_path:
        overrides["base_path"] = args.base_path
    if args.raw_path:
        overrides["raw_path"] = args.raw_path
    if args.checkpoint_path:
        overrides["checkpoint_path"] = args.checkpoint_path
    if args.quarantine_path:
        overrides["quarantine_path"] = args.quarantine_path
    if args.data_referencia:
        overrides["data_referencia"] = datetime.strptime(args.data_referencia, "%Y-%m-%d").date()
    if args.batch_id:
        overrides["batch_id"] = args.batch_id

    config = get_config(**overrides)
    logger = get_logger("pipeline.main", batch_id=config.batch_id)
    logger.info("iniciando execução do pipeline", **config.as_dict())

    spark = get_spark()
    layers = {layer.strip() for layer in args.layers.split(",")}

    try:
        if "bronze" in layers:
            run_bronze_ingestion(spark, config)
        if "silver" in layers:
            run_silver_processing(spark, config)
        if "gold" in layers:
            run_gold_processing(spark, config)
    except Exception:
        logger.exception("pipeline falhou")
        raise
    finally:
        if config.env == "local":
            spark.stop()

    logger.info("pipeline concluído com sucesso")
    return 0


if __name__ == "__main__":
    sys.exit(main())
