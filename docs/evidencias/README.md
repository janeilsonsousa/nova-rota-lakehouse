# Evidências de execução

Rodei cada notebook separado (um por vez, na interface do Databricks) e depois o job completo em sequência, tudo em compute Serverless. Os arquivos abaixo estão numerados na ordem em que o pipeline roda.

## Índice

| Arquivo | O que mostra |
|---|---|
| `01_setup_ambiente.jpg` | notebook 00 rodado sozinho — cria catálogo/schemas/Volume, valida import do `src/` |
| `02_bronze_ingestion.jpg` | notebook 01 sozinho — as 6 tabelas Bronze criadas, com contagem de linhas e amostra de `bronze_transacoes` |
| `03_silver_processing.jpg` | notebook 02 sozinho — histórico SCD2 de um cliente com 2 versões (`flag_vigente` mudando) |
| `04_gold_data_products.jpg` | notebook 03 sozinho — volumetria das 8 tabelas Ouro |
| `05_sql_analysis.jpg` | notebook 04 sozinho — query de segmentação NTILE/PERCENT_RANK rodando contra as views |
| `06_job_completo.jpg` | os 5 notebooks (00→04) encadeados num job só, todos verdes, ~6m30s |
| `07_catalog_explorer.jpg` | Catalog Explorer depois do job: schema bronze (6 tabelas) e gold (8 tabelas) expandidos |
| `08_volumetria_sql.txt` | contagem de linhas e checagem de duplicidade rodada via SQL Warehouse logo depois do job |
| `local_pipeline_run.log` | log JSON de uma execução local ponta a ponta (`python -m src.run_pipeline`) |
| `pytest_full_suite.log` | saída da suíte completa (28 testes: 14 unitários + 14 de transformação) |

## Sobre os números

Bronze é append-only e a ingestão é idempotente por hash de arquivo — rodar de novo com o mesmo arquivo não duplica. Só que como venho rodando notebook/job várias vezes ao longo do desenvolvimento (cada vez com um Repo git diferente sincronizado ou reprocessando), as contagens em `bronze_transacoes` etc. vão crescendo ao longo do projeto — é o comportamento esperado de uma tabela append-only sendo alimentada repetidamente com a mesma massa de dados sintética, não é bug nem duplicidade real (isso é conferido explicitamente na query "duplicidade `id_transacao` na silver", que dá sempre 0).

## Como reproduzir

```bash
python -m src.run_pipeline --env local --batch-id evidencia-001
python -m pytest -q
```

Em Databricks: importa `notebooks/` via Repos e roda 00 → 01 → 02 → 03 → 04 em sequência (cada um pode rodar sozinho ou como job encadeado).
