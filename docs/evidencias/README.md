# Evidências de execução

Logs de execução, prints do Databricks com os notebooks rodados, e resultado dos testes.

## Índice

| Arquivo | Descrição |
|---|---|
| `local_pipeline_run.log` | log JSON de uma execução local ponta a ponta, com prova de idempotência (reexecução processa 0 linhas) |
| `pytest_full_suite.log` | saída da suíte completa (28 testes) |
| `databricks_03_gold_volumetria.jpg` | notebook 03 rodado no Databricks Serverless, 8 tabelas Ouro criadas |
| `databricks_04_sql_ntile_percentrank.jpg` | notebook 04, query de segmentação NTILE/PERCENT_RANK |
| `databricks_05_catalog_bronze.jpg` | Catalog Explorer — schema bronze, 6 tabelas |
| `databricks_06_catalog_silver.jpg` | Catalog Explorer — schema silver, 6 tabelas SCD2 |
| `databricks_07_catalog_gold_arvore_completa.jpg` | árvore completa do catálogo, 13 tabelas nas 3 camadas |

## Como reproduzir

```bash
python -m src.run_pipeline --env local --batch-id evidencia-001
python -m pytest -q
```

Em Databricks: importa `notebooks/` via Repos e roda 00 → 01 → 02 → 03 → 04 em sequência.
