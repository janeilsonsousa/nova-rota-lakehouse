# Arquitetura

## 1. Visão geral

Medallion (Bronze → Prata → Ouro) em Databricks + Delta Lake, PySpark modular (não notebook monolítico), um ponto de entrada só (`src/run_pipeline.py`).

```
                         ┌─────────────────────────────────────────┐
                         │      data/raw/nova_rota_input/           │
                         │  clientes_cdc.csv, contas_cdc.csv,        │
                         │  cartoes_cdc.csv, transacoes/*.csv,       │
                         │  eventos_risco.csv, estornos.csv          │
                         └───────────────────┬───────────────────────┘
                                             │  ingestão incremental idempotente
                                             │  (src/ingestion/bronze.py + control.py)
                                             ▼
┌───────────────────────────────────────────────────────────────────────────┐
│ BRONZE  (dado bruto + metadados de lineage)                                │
│  bronze_clientes_cdc · bronze_contas_cdc · bronze_cartoes_cdc              │
│  bronze_transacoes · bronze_eventos_risco · bronze_estornos                │
│  + _ingestion_control (controle de arquivos processados)                   │
└───────────────────────────────────┬─────────────────────────────────────-─┘
                                     │  quality gate + SCD2 + MERGE incremental
                                     │  (src/quality/rules.py, src/silver/*)
                                     ▼
┌───────────────────────────────────────────────────────────────────────────┐
│ PRATA  (limpo, tipado, íntegro, versionado)                                │
│  silver_clientes (SCD2) → silver_contas (SCD2) → silver_cartoes (SCD2)     │
│         └──────────────────────┬──────────────────────────┘                │
│                                 ▼                                          │
│  silver_transacoes (MERGE por id_transacao) → silver_eventos_risco         │
│                                              → silver_estornos             │
│  + _quarentena/{entidade} (registros reprovados, com motivo)               │
└───────────────────────────────────┬─────────────────────────────────────-─┘
                                     │  join ponto-no-tempo + agregações
                                     │  (src/gold/*)
                                     ▼
┌───────────────────────────────────────────────────────────────────────────┐
│ OURO  (data products para BI e Data Science)                               │
│  gold_fato_transacao  (grão: 1 por id_transacao, atributos ponto-no-tempo) │
│  gold_dim_cliente / gold_dim_conta / gold_dim_cartao / gold_dim_estab.     │
│  gold_cliente_mes        (comportamento mensal)                            │
│  gold_indicadores_risco  (risco/estorno/chargeback mensal)                 │
│  gold_features_cliente   (snapshot RFM + percentis p/ Data Science)        │
└───────────────────────────────────────────────────────────────────────────┘
```

A ordem de dependência é respeitada pelos orquestradores: clientes → contas → cartões → transações → eventos_risco/estornos na Prata; fato → dimensões → agregados na Ouro. É o que garante integridade referencial em cascata (uma conta só é válida se o cliente já existe na Prata).

## 2. Estrutura do repositório

```
src/
  config/        parametrização central (env, catálogo/schema, paths, datas, batch_id)
  ingestion/     Bronze: ingestão incremental + tabela de controle
  quality/       regras de qualidade + quarentena (genérico, usado por todas as entidades)
  silver/        1 módulo por entidade + scd2.py (genérico) + common.py
  gold/          1 módulo por data product + common.py (join ponto-no-tempo, merge por chave composta)
  utils/         logging, sessão Spark portátil, helpers
  run_pipeline.py  ponto de entrada (CLI)
tests/
  transform/     testes de transformação com massa pequena (Spark real)
  unit/          testes de lógica pura (sem Spark)
sql/             consultas SQL avançadas
notebooks/       execução/demonstração em Databricks
docs/            este documento + decisions.md (ADRs) + data_contracts.md
```

## 3. Idempotência

Toda camada é idempotente:

- **Bronze**: controle por hash de conteúdo, arquivo já processado não repete (a menos que `run_mode=full`).
- **Prata**: SCD2 reconstrói a timeline da chave do zero a cada execução; transações/eventos/estornos usam MERGE por chave natural.
- **Ouro**: `gold_fato_transacao` faz MERGE por `id_transacao`; os agregados mensais recalculam o grupo inteiro pela chave composta afetada.

Todo módulo de teste tem um caso de reexecução com o mesmo `batch_id` conferindo contagem estável.

## 4. Cenários obrigatórios — onde cada um é resolvido

| Cenário | Onde |
|---|---|
| Cliente com várias contas e cartões | FK natural (`id_cliente` em contas, `id_conta` em cartões) |
| Cartão muda de status ao longo do tempo | SCD2 em `silver_cartoes` |
| Cartão cancelado não conta em métrica futura mas preserva histórico | ADR-05: fato preserva tudo, agregados decidem o corte |
| Transação estornada não soma no valor líquido | `gold_fato_transacao.valor_liquido = 0` quando `flag_estornada` |
| Dado cadastral reflete versão vigente na data da transação | Join ponto-no-tempo (`src/gold/common.py::join_ponto_no_tempo`) |
| Arquivos fora de ordem | ADR-02/03/06 — timeline SCD2 reordenada + agregados recomputados |
| Mesma `id_transacao` em cargas diferentes | MERGE por `id_transacao` em `silver_transacoes` |

## 5. Performance, operação e governança

**Particionamento**: `gold_fato_transacao` é candidata a Liquid Clustering por `dt_transacao, id_cliente_na_data` em produção (evita ter que escolher granularidade de partição fixa e reescrever depois). No volume deste desafio, particionar geraria arquivo pequeno demais, então não fiz. Dimensões SCD2 são pequenas, só `OPTIMIZE` periódico já basta.

**Arquivos pequenos**: ingestão incremental gera bastante arquivo pequeno por natureza. Mitigar com `OPTIMIZE` agendado fora da janela de ingestão + `optimizeWrite`/`autoCompact` habilitados no cluster.

**Z-ORDER / Liquid Clustering**: `gold_fato_transacao` (maior volume) se beneficiaria de Z-ORDER (ou Liquid Clustering) em `id_cliente_na_data, dt_transacao` — as duas colunas mais usadas em WHERE/JOIN nas queries de `sql/advanced_queries.sql`.

**Joins**: os joins ponto-no-tempo (fato × SCD2) esperam `BroadcastHashJoin` porque as dimensões são pequenas o suficiente pra caber no threshold de broadcast — evita shuffle da fato. Validado com `EXPLAIN FORMATTED` (bloco 8 do SQL).

**Falhas e observabilidade**: `run_pipeline.py` nunca mascara exceção, propaga pro orquestrador marcar falha. Log estruturado em JSON com `batch_id` em toda linha. Cada etapa loga contagem processada/rejeitada — em produção isso alimentaria um painel, com alerta se a taxa de quarentena de alguma entidade passar de um limiar (ex. 5%).

**Unity Catalog**: catálogo `nova_rota` (1 por ambiente numa config real), schemas `bronze`/`silver`/`gold` batendo com `PipelineConfig`. Permissão por camada (não provisionada aqui, mas o desenho): bronze restrito à engenharia, silver leitura pra analistas, gold leitura ampla pra BI/DS, escrita só pelo pipeline nas três.

**Agendamento**: 1 Job com 3 tasks em sequência (bronze → silver → gold), cada uma chamando `run_pipeline.py --layers <camada>` com parâmetros do Job. Retry automático + alerta em falha final. Cron fora do pico transacional, com gatilho intradiário extra se precisar de dado mais fresco.

## 6. Auto Loader — por que não entrou

Ver ADR-01 em `docs/decisions.md`.

## 7. Trade-offs

| Implementado | Desenhado, não implementado | Em produção |
|---|---|---|
| Ingestão incremental com controle de hash | Auto Loader / streaming real | Migrar pra `cloudFiles` com object storage cloud |
| SCD2 completo com reconstrução de timeline | — | Mesma abordagem, de olho no custo em volume alto |
| MERGE incremental idempotente em toda Prata/Ouro | — | Mesma abordagem |
| Quarentena com motivo por regra | Alerta automatizado por taxa de quarentena | Job de observabilidade dedicado |
| Testes de transformação com massa pequena | Teste de carga/volume | Suite de performance em staging |
| Unity Catalog provisionado (catálogo, schemas, 13 tabelas — ADR-09) | Permissão granular por papel | Workspace corporativo + script de setup |
