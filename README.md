# NovaRota Lakehouse

Data product Lakehouse (Bronze → Prata → Ouro) para a Cooperativa NovaRota,
em Databricks + Delta Lake. Case técnico sênior de Engenharia de Dados.

> Documentação complementar: [`docs/architecture.md`](docs/architecture.md)
> (arquitetura, performance, governança), [`docs/decisions.md`](docs/decisions.md)
> (decisões técnicas e trade-offs) e [`docs/data_contracts.md`](docs/data_contracts.md)
> (grão/chaves/regras de cada tabela).

## O que este projeto entrega

- Camada **Bronze**: ingestão incremental idempotente de 6 fontes (clientes,
  contas, cartões, transações, eventos de risco, estornos), preservando
  dado bruto e evolução de schema.
- Camada **Prata**: qualidade de dados com quarentena, histórico SCD Tipo 2
  para clientes/contas/cartões, MERGE incremental idempotente,
  integridade referencial em cascata, tratamento de dados atrasados e
  duplicados.
- Camada **Ouro**: fato transacional com resolução ponto-no-tempo, 4
  dimensões, métricas comportamentais mensais, indicadores de risco e
  features para Data Science.
- SQL avançado (CTEs, `ROW_NUMBER`, `LAG`/`LEAD`, `FIRST_VALUE`/`LAST_VALUE`,
  `NTILE`/`PERCENT_RANK`, detecção de anomalia, `MERGE INTO`) em
  [`sql/advanced_queries.sql`](sql/advanced_queries.sql).
- 28 testes automatizados (14 unitários + 14 de transformação com massa
  pequena) cobrindo as regras de negócio críticas.

## Estrutura do repositório

```
src/            código do pipeline (ver docs/architecture.md para o mapa completo)
tests/unit/     testes unitários puros (sem Spark, <1s)
tests/transform/ testes de transformação com massa pequena (Spark real)
sql/            consultas SQL avançadas
notebooks/      notebooks Databricks (camada de execução/demonstração)
docs/           arquitetura, decisões técnicas, contratos de dados
data/raw/       massa de dados sintética de entrada
tools/hadoop-win/ winutils vendorizado (só para dev local no Windows)
```

## Pré-requisitos

- Python 3.10–3.12
- Java 17 (JDK) — necessário para o Spark local
- Uma conta Databricks (Free Edition, trial ou workspace corporativo) para
  a execução "real" com evidências — ver seção *Executando em Databricks*

## Setup local (desenvolvimento e testes)

```bash
python -m venv .venv
# Windows
.venv\Scripts\pip install -r requirements.txt
# Linux/Mac
.venv/bin/pip install -r requirements.txt
```

No **Windows**, o Spark local precisa do `winutils.exe` — já vendorizado em
`tools/hadoop-win/` e detectado automaticamente
(`src/utils/spark_session.py`). Só é necessário ter o JDK 17 instalado e no
`PATH`/`JAVA_HOME`.

### Rodando o pipeline localmente

```bash
python -m src.run_pipeline --env local --run-mode incremental
```

Isso cria `data/lakehouse/` (Delta local) com as 3 camadas completas a
partir da massa em `data/raw/nova_rota_input/`. Parâmetros disponíveis:

```bash
python -m src.run_pipeline --help
```

| Parâmetro | Default | Descrição |
|---|---|---|
| `--env` | `local` | `local` ou `databricks` |
| `--catalog` | `nova_rota` | Catálogo Unity Catalog (só `--env databricks`) |
| `--run-mode` | `incremental` | `incremental` ou `full` (backfill/reprocessamento total) |
| `--data-referencia` | hoje | `YYYY-MM-DD`, propagado à configuração |
| `--batch-id` | gerado (uuid) | Identificador do lote, atravessa Bronze→Prata→Ouro |
| `--layers` | `bronze,silver,gold` | Subconjunto de camadas a rodar |

### Rodando os testes

```bash
python -m pytest tests/unit -q          # rápidos, sem Spark
python -m pytest tests/transform -q     # com Spark real, massa pequena (~4min no Windows local)
python -m pytest -q                     # tudo
```

## Executando em Databricks (evidência real)

1. Crie/abra um workspace Databricks (Free Edition serve:
   https://www.databricks.com/learn/free-edition).
2. Suba este repositório via **Repos** (Git) ou importe os notebooks de
   `notebooks/` diretamente.
3. Crie um cluster (ou use um SQL Warehouse para as queries de
   `sql/advanced_queries.sql`) com Databricks Runtime 14.x/15.x LTS
   (Spark 3.5.x — mesma versão usada em dev local).
4. Rode, em ordem, os notebooks de `notebooks/` (cada um documenta o que
   demonstra) **ou** direto via terminal do cluster:
   ```
   %pip install -r requirements.txt   # normalmente desnecessário: runtime já traz pyspark/delta
   python -m src.run_pipeline --env databricks --catalog nova_rota
   ```
5. Capturas de tela / logs de execução ficam em `docs/evidencias/` (ver
   `docs/evidencias/README.md` para o índice).

## Premissas e limitações

- A massa de dados é sintética (ver
  `data/raw/nova_rota_input/README_DADOS.md` para o desenho completo dos
  cenários simulados: duplicidade, dados inválidos, integridade
  referencial quebrada, atraso, evolução de schema).
- Estorno tratado como total (layout de origem não traz valor parcial) —
  ver ADR-04 em `docs/decisions.md`.
- Auto Loader não implementado (ambiente sem object storage cloud
  configurável de forma reproduzível) — desenho completo e justificativa em
  ADR-01.
- CDC com operação de delete físico (`operacao='D'`) não está presente na
  massa e não é tratado — ver `docs/decisions.md`, seção final.

## Próximos passos (produção)

Ver `docs/architecture.md`, seção 7 (trade-offs) e `docs/decisions.md` para
o detalhamento completo — resumo: Auto Loader real, Unity Catalog
provisionado com permissões por camada, Databricks Workflows agendado,
alertas automatizados de taxa de quarentena, testes de volume/performance.
