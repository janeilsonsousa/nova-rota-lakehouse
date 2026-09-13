# Arquitetura — NovaRota Lakehouse

## 1. Visão geral

Arquitetura Medallion (Bronze → Prata → Ouro) em Databricks + Delta Lake,
implementada em PySpark modular (não notebooks monolíticos), orquestrada por
um único ponto de entrada parametrizável (`src/run_pipeline.py`).

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

A ordem de dependência é estritamente respeitada pelos orquestradores
(`run_silver_processing`, `run_gold_processing`): clientes → contas →
cartões → transações → eventos_risco/estornos na Prata; fato → dimensões →
agregados na Ouro. Isso é o que permite validar integridade referencial em
cascata (uma conta só é válida se o cliente já existe na Prata).

## 2. Estrutura do repositório

```
src/
  config/        parametrização central (env, catálogo/schema, paths, datas, batch_id)
  ingestion/     Bronze: ingestão incremental + tabela de controle
  quality/       motor de regras de qualidade + quarentena (genérico, reusado por todas as entidades)
  silver/        1 módulo por entidade + scd2.py (genérico) + common.py
  gold/          1 módulo por data product + common.py (join ponto-no-tempo, merge por chave composta)
  utils/         logging estruturado, sessão Spark portátil, helpers
  run_pipeline.py  ponto de entrada único (CLI)
tests/
  transform/     testes de transformação com massa pequena (Spark real)
  unit/          testes de lógica pura (sem Spark)
sql/             consultas SQL avançadas exigidas pelo desafio
notebooks/       camada de execução/demonstração em Databricks
docs/            este documento + decisions.md (ADRs) + data_contracts.md
```

Separação deliberada: regras de negócio (silver/gold), regras de qualidade
(quality/), funções utilitárias (utils/) e execução (run_pipeline.py) nunca
se misturam no mesmo arquivo — cada módulo de entidade (ex.
`src/silver/clientes.py`) só orquestra chamadas às camadas reutilizáveis.

## 3. Idempotência

Toda camada é idempotente por desenho, não por acidente:

- **Bronze**: tabela de controle por hash de conteúdo — arquivo já
  processado não é reprocessado (a menos que `run_mode=full`).
- **Prata**: SCD2 reconstrói a timeline da chave do zero a cada execução
  (determinístico); transações/eventos/estornos usam MERGE INTO por chave
  natural — reenviar o mesmo dado não duplica.
- **Ouro**: `gold_fato_transacao` usa MERGE por `id_transacao`;
  `gold_cliente_mes`/`gold_indicadores_risco` recalculam o grupo inteiro
  por chave composta afetada.

Testado explicitamente: todos os módulos de teste incluem um caso de
reexecução com o mesmo `batch_id` verificando contagem de linhas estável.

## 4. Cenários obrigatórios (requisito 9) — onde cada um é resolvido

| Cenário | Onde é tratado |
|---|---|
| Cliente com várias contas e vários cartões | Modelo relacional natural (FK `id_cliente` em contas, `id_conta` em cartões); sem tratamento especial necessário |
| Cartão muda de status ao longo do tempo | SCD2 em `silver_cartoes` |
| Cartão cancelado não compõe métricas futuras mas preserva histórico | ADR-05 (`docs/decisions.md`): fato preserva tudo; agregados decidem o corte |
| Transação estornada não soma no valor líquido | `gold_fato_transacao.valor_liquido = 0` quando `flag_estornada` |
| Dado cadastral reflete versão vigente na data da transação | Join ponto-no-tempo (`src/gold/common.py::join_ponto_no_tempo`) |
| Arquivos fora de ordem | ADR-02/ADR-03/ADR-06 — timeline SCD2 reordenada + agregados recomputados por grupo afetado |
| Mesma `id_transacao` em cargas diferentes | MERGE INTO por `id_transacao` em `silver_transacoes` |

## 5. Performance, operação e governança (requisito 8)

### Particionamento e clustering
- `silver_transacoes`/`gold_fato_transacao`: candidatas a particionamento
  físico por `dt_transacao` (mês) em produção — volume real de uma
  cooperativa geraria partições grandes o suficiente para compensar; no
  volume deste desafio, particionar seria contraproducente (arquivos
  pequenos demais). Preferência declarada: **Liquid Clustering por
  `dt_transacao, id_cliente_na_data`** em vez de partição Hive tradicional
  — evita o problema clássico de partição fixa (necessidade de escolher
  granularidade upfront, reescrita cara para mudar) e permite clustering
  multi-dimensional (data + cliente, os dois padrões de acesso mais
  comuns: "transações do mês" e "transações de um cliente").
- Dimensões SCD2 (`silver_clientes/contas/cartoes`): pequenas, sem
  necessidade de particionamento; `OPTIMIZE` periódico é suficiente.

### Arquivos pequenos
- A ingestão incremental Bronze por natureza gera muitos arquivos pequenos
  (1 write por fonte por execução). Mitigação: `OPTIMIZE` agendado
  (diário, fora da janela de ingestão) nas tabelas Bronze/Prata de maior
  volume, e `spark.databricks.delta.optimizeWrite.enabled=true` +
  `autoCompact.enabled=true` no cluster/warehouse (compactação automática
  no write, padrão recomendado em Databricks Runtime atual).

### OPTIMIZE / Z-ORDER / Liquid Clustering
- Tabelas pequenas (dimensões, agregados mensais): `OPTIMIZE` simples.
- `gold_fato_transacao` (maior volume, mais consultada por filtro de
  cliente e por período): Z-ORDER BY (ou Liquid Clustering, se no
  Databricks Runtime ≥ 13.3) em `id_cliente_na_data, dt_transacao` — são as
  duas colunas mais usadas em `WHERE`/`JOIN` (ver `sql/advanced_queries.sql`,
  praticamente toda query filtra ou agrupa por uma das duas).

### Estratégia de joins (evidência em `sql/advanced_queries.sql`, bloco 8)
- Os joins ponto-no-tempo (fato × dimensão SCD2) são projetados para
  `BroadcastHashJoin`: as dimensões (clientes/contas/cartões) são pequenas
  o suficiente (mesmo em produção real, dezenas de milhares de linhas) para
  caber em `spark.sql.autoBroadcastJoinThreshold` — evita shuffle da tabela
  fato (a maior) nesse join. Validado com `EXPLAIN FORMATTED` (ver SQL
  citado); em produção, monitorar via Spark UI se o plano se mantém
  broadcast conforme a dimensão cresce, e ajustar o threshold ou forçar
  `/*+ BROADCAST(dim) */` se necessário.

### Monitoramento de falhas, volumetria e qualidade
- **Falhas**: `src/run_pipeline.py` nunca mascara exceção — propaga para o
  orquestrador (Databricks Workflows) marcar a execução como falha e
  alertar. Logs estruturados em JSON (`src/utils/logging_utils.py`) com
  `batch_id` em toda linha, prontos para serem coletados por uma tabela de
  log/observabilidade ou exportados para um sistema externo (ex. tabela
  Delta de logs, Datadog, Log Analytics).
- **Volumetria**: cada etapa loga contagem de linhas processadas/rejeitadas
  (`ingestion bronze finalizada`, `quality gate aplicado`, `SCD2 aplicado`,
  `MERGE concluído`) — em produção, essas métricas alimentariam um painel
  de observabilidade (linhas por execução, taxa de quarentena por entidade
  ao longo do tempo — um salto repentino indica problema na origem).
- **Qualidade**: taxa de quarentena por entidade e por motivo é a métrica
  central; um alerta automatizado dispararia se a taxa de quarentena de
  qualquer entidade ultrapassar um limiar (ex. >5% dos registros do lote).

### Unity Catalog — catálogos, schemas e permissões
- **Catálogo** `nova_rota` (1 por ambiente: `nova_rota_dev`, `nova_rota_prd`
  em uma configuração real com múltiplos ambientes).
- **Schemas** `bronze`, `silver`, `gold` dentro do catálogo — replica a
  estrutura de camadas já usada no código (`PipelineConfig.bronze_schema`
  etc.), então a troca de `env=local` para `env=databricks` é só uma
  questão de configuração, sem mudar nenhuma lógica.
- **Permissões** (princípio de menor privilégio):
  - `bronze`: acesso restrito à engenharia de dados (dado bruto, pode ter
    PII sem tratamento).
  - `silver`: leitura para analistas de dados/qualidade; escrita só pelo
    pipeline (service principal).
  - `gold`: leitura ampla para BI e Data Science; escrita só pelo pipeline.
  - Volumes Unity Catalog para os arquivos de origem (`raw_path` em
    produção apontaria para um Volume, não um path local).

### Agendamento (Databricks Workflows)
- 1 Job com 3 tasks em sequência (`bronze_ingest` → `silver_process` →
  `gold_process`), cada task chamando `python -m src.run_pipeline --layers
  bronze` (e assim por diante) com parâmetros do Job (catálogo, schema,
  `run_mode`). Retries: 1 automático com backoff, alerta (e-mail/Teams/Slack)
  em falha final.
- Cron sugerido: fora do horário de pico transacional da cooperativa (ex.
  madrugada), com uma segunda execução intradiária se a área de negócio
  precisar de dados mais frescos — o mesmo padrão de "cron + gatilho
  adicional" é comum em plataformas de dados maduras (ver também o
  documento de referência do Data Hub em anexo ao desafio, que usa Gold
  2x/dia com gatilho pós-ingestão opcional).
- Parametrização por ambiente via Job parameters, não hardcoded no notebook
  — mesma filosofia de `PipelineConfig`.

## 6. Auto Loader — por que não foi implementado

Ver ADR-01 em `docs/decisions.md` para a justificativa completa.

## 7. Trade-offs — resumo executivo

| Implementado | Desenhado (não implementado) | Seria feito em produção |
|---|---|---|
| Ingestão incremental com controle de hash | Auto Loader / streaming real | Migrar para `cloudFiles` com object storage cloud |
| SCD2 completo com reconstrução de timeline | — | Mesma abordagem, monitorando custo em volume alto |
| MERGE incremental idempotente em toda a Prata/Ouro | — | Mesma abordagem |
| Quarentena com motivo por regra | Alertas automatizados por taxa de quarentena | Job de observabilidade dedicado |
| Testes de transformação com massa pequena | Testes de carga/volume | Suite de performance em ambiente de staging |
| Unity Catalog desenhado | Provisionamento real (exige admin de conta) | Terraform/script de setup do catálogo |
