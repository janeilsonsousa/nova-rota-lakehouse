# Evidências de execução

Este diretório reúne as evidências mínimas de execução exigidas pelo
desafio: logs de execução, capturas de tela do workspace Databricks com os
notebooks executados célula a célula, e resultados de testes.

## Índice

| Arquivo | Descrição |
|---|---|
| `local_pipeline_run.log` | Log completo (JSON estruturado) de uma execução local ponta a ponta (`python -m src.run_pipeline`), Bronze → Prata → Ouro, incluindo prova de idempotência (reexecução processando 0 linhas novas). |
| `pytest_full_suite.log` | Saída da suíte completa de testes (28 testes: 14 unitários + 14 de transformação). |
| `databricks_03_gold_volumetria.jpg` | Captura do notebook `03_gold_data_products` executado no Databricks (Serverless), mostrando as 8 tabelas Ouro criadas com sucesso. |
| `databricks_04_sql_ntile_percentrank.jpg` | Captura do notebook `04_sql_analysis` executado no Databricks, resultado da query de segmentação por NTILE/PERCENT_RANK. |
| `databricks_05_catalog_bronze.jpg` | Catalog Explorer — schema `nova_rota.bronze` com as 6 tabelas Bronze registradas (uma por fonte de CDC/evento). |
| `databricks_06_catalog_silver.jpg` | Catalog Explorer — schema `nova_rota.silver` com as 6 dimensões/fatos Prata (SCD2) registradas. |
| `databricks_07_catalog_gold_arvore_completa.jpg` | Catalog Explorer com a árvore do catálogo `nova_rota` expandida (bronze/silver/gold), evidenciando as 13 tabelas registradas nas três camadas do medallion de uma só vez. |

## Como reproduzir

```bash
python -m src.run_pipeline --env local --batch-id evidencia-001
python -m pytest -q
```

Em Databricks: importar `notebooks/` via Repos e rodar `00` → `01` → `02`
→ `03` → `04` em sequência (cada notebook documenta, em células Markdown, o
que está sendo demonstrado).
