# NovaRota Lakehouse

Lakehouse Bronze → Prata → Ouro pra Cooperativa NovaRota, em Databricks + Delta Lake.

> Documentação extra: [`docs/architecture.md`](docs/architecture.md) (arquitetura/performance/governança), [`docs/decisions.md`](docs/decisions.md) (decisões técnicas) e [`docs/data_contracts.md`](docs/data_contracts.md) (grão/chaves de cada tabela).

## O que tem aqui

- **Bronze**: ingestão incremental idempotente de 6 fontes (clientes, contas, cartões, transações, eventos de risco, estornos), preservando dado bruto e evolução de schema.
- **Prata**: quality gate com quarentena, SCD2 pra clientes/contas/cartões, MERGE incremental, integridade referencial em cascata, dado atrasado e duplicado.
- **Ouro**: fato transacional com resolução ponto-no-tempo, 4 dimensões, métricas mensais, indicadores de risco e features pra Data Science.
- SQL avançado (CTEs, ROW_NUMBER, LAG/LEAD, FIRST_VALUE/LAST_VALUE, NTILE/PERCENT_RANK, detecção de anomalia, MERGE INTO) em [`sql/advanced_queries.sql`](sql/advanced_queries.sql).
- 28 testes automatizados (14 unitários + 14 de transformação) cobrindo as regras de negócio principais.

## Estrutura

```
src/            código do pipeline (mapa completo em docs/architecture.md)
tests/unit/     testes unitários, sem Spark
tests/transform/ testes de transformação com Spark real, massa pequena
sql/            queries SQL avançadas
notebooks/      notebooks Databricks (execução/demonstração)
docs/           arquitetura, decisões, contratos de dados
data/raw/       massa de dados sintética
tools/hadoop-win/ winutils vendorizado, só pra dev local no Windows
```

## Pré-requisitos

- Python 3.10–3.12
- Java 17 (JDK), pro Spark local
- Conta Databricks (Free Edition serve) pra rodar de verdade e gerar evidência

## Setup local

```bash
python -m venv .venv
# Windows
.venv\Scripts\pip install -r requirements.txt
# Linux/Mac
.venv/bin/pip install -r requirements.txt
```

No Windows o Spark local precisa do `winutils.exe` — já vem em `tools/hadoop-win/` e é detectado sozinho (`src/utils/spark_session.py`). Só precisa ter JDK 17 no PATH/JAVA_HOME.

### Rodando o pipeline

```bash
python -m src.run_pipeline --env local --run-mode incremental
```

Isso cria `data/lakehouse/` com as 3 camadas a partir da massa em `data/raw/nova_rota_input/`.

```bash
python -m src.run_pipeline --help
```

| Parâmetro | Default | Descrição |
|---|---|---|
| `--env` | `local` | `local` ou `databricks` |
| `--catalog` | `nova_rota` | catálogo Unity Catalog (só `--env databricks`) |
| `--run-mode` | `incremental` | `incremental` ou `full` (backfill) |
| `--data-referencia` | hoje | `YYYY-MM-DD` |
| `--batch-id` | gerado (uuid) | id do lote, atravessa Bronze→Prata→Ouro |
| `--layers` | `bronze,silver,gold` | subconjunto de camadas a rodar |

### Testes

```bash
python -m pytest tests/unit -q          # rápidos, sem Spark
python -m pytest tests/transform -q     # Spark real, ~4min no Windows local
python -m pytest -q                     # tudo
```

## Rodando em Databricks

A evidência em `docs/evidencias/` foi gerada num workspace Databricks Free Edition, em compute Serverless (sem cluster, é o que não tem custo nesse tipo de conta). Isso pediu 3 ajustes de compatibilidade documentados no ADR-09 (`docs/decisions.md`): sem RDD/SparkContext direto, sem `.persist()`, storage físico via Unity Catalog Volume em vez do Workspace filesystem (read-only pra dado em Serverless).

1. Abra um workspace Databricks (Free Edition: https://www.databricks.com/learn/free-edition).
2. Suba o repo via Repos (Git) ou importe os notebooks de `notebooks/` direto.
3. Rode em ordem: `00_setup_ambiente` → `01_bronze_ingestion` → `02_silver_processing` → `03_gold_data_products` → `04_sql_analysis`. Em Serverless já roda sem selecionar cluster; num cluster clássico (Databricks Runtime 14.x/15.x LTS) funciona igual, os ajustes do ADR-09 só evitam API que o Serverless bloqueia.
4. Alternativa via terminal do cluster:
   ```
   %pip install -r requirements.txt   # geralmente desnecessário, o runtime já traz pyspark/delta
   python -m src.run_pipeline --env databricks --catalog nova_rota
   ```
5. No final de cada camada o pipeline tenta registrar as tabelas no Unity Catalog por nome (`nova_rota.<camada>.<tabela>`) — mecanismo no ADR-09, árvore completa em `docs/evidencias/databricks_07_catalog_gold_arvore_completa.jpg`.
6. Prints e logs de execução ficam em `docs/evidencias/` (índice em `docs/evidencias/README.md`).

## Premissas e limitações

- Massa sintética — desenho completo dos cenários (duplicidade, dado inválido, integridade quebrada, atraso, evolução de schema) em `data/raw/nova_rota_input/README_DADOS.md`.
- Estorno tratado como total, o layout de origem não traz valor parcial (ADR-04 em `docs/decisions.md`).
- Auto Loader não implementado, não tem object storage cloud configurável de forma reproduzível aqui (ADR-01).
- CDC com delete físico (`operacao='D'`) não está na massa e não é tratado (ver `docs/decisions.md`, seção final).

## Próximos passos pra produção

Detalhe completo em `docs/architecture.md` (seção 7) e `docs/decisions.md`. Resumo: Auto Loader real, Unity Catalog com permissões por camada, Databricks Workflows agendado, alerta de taxa de quarentena, teste de volume/performance.
