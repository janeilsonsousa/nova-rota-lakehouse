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
| `databricks_08_job_run_completo.jpg` | job com os 5 notebooks (00→04) rodados em sequência no Serverless, todos verdes, 7m21s |
| `databricks_09_catalog_completo_20_tabelas.jpg` | catálogo depois da camada Ouro completa: 6 bronze + 6 silver + 8 gold = 20 tabelas |
| `databricks_volumetria_sql.txt` | volumetria e checagem de duplicidade rodada via SQL Warehouse depois do run acima |

## Sobre o job `databricks_08`

Rodei esse job pra validar o pipeline inteiro de novo depois de mexer nos comentários do código (deixar
menos "cara de IA"). Na primeira tentativa o notebook `01_bronze_ingestion` quebrou com
`ImportError: cannot import name 'UTC' from 'datetime'` — o Databricks Serverless roda Python 3.10, e
o `datetime.UTC` só existe a partir do 3.11. O `pyproject.toml` já declarava `requires-python >=3.10`, só
que o `ruff` estava configurado pra sugerir sintaxe de 3.11 (foi ele que sugeriu o `UTC` em algum commit
anterior). Troquei por `timezone.utc`, corrigi o `target-version` do ruff pra `py310`, e no segundo run
passou tudo (ver `docs/decisions.md`, ADR-10).

## Como reproduzir

```bash
python -m src.run_pipeline --env local --batch-id evidencia-001
python -m pytest -q
```

Em Databricks: importa `notebooks/` via Repos e roda 00 → 01 → 02 → 03 → 04 em sequência.
