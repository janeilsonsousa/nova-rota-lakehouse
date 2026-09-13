# Evidências de execução

Este diretório reúne as evidências mínimas de execução exigidas pelo
desafio: logs de execução, capturas de tela do workspace Databricks com os
notebooks executados célula a célula, e resultados de testes.

## Índice

| Arquivo | Descrição |
|---|---|
| `local_pipeline_run.log` | Log completo (JSON estruturado) de uma execução local ponta a ponta (`python -m src.run_pipeline`), Bronze → Prata → Ouro, incluindo prova de idempotência (reexecução processando 0 linhas novas). |
| `pytest_full_suite.log` | Saída da suíte completa de testes (28 testes: 14 unitários + 14 de transformação). |
| `databricks_*.png` | Capturas de tela do workspace Databricks Free Edition com os notebooks `notebooks/00`–`04` executados (a preencher após a execução real no workspace). |

## Como reproduzir

```bash
python -m src.run_pipeline --env local --batch-id evidencia-001
python -m pytest -q
```

Em Databricks: importar `notebooks/` via Repos e rodar `00` → `01` → `02`
→ `03` → `04` em sequência (cada notebook documenta, em células Markdown, o
que está sendo demonstrado).
