# Contratos de dados

Grão, chaves e regras de negócio de cada tabela — referência pra quem consumir a Ouro ou mexer na Prata.

## Convenções gerais

- **Nomenclatura**: `{camada}_{entidade}`, ex. `silver_clientes`,
  `gold_fato_transacao`.
- **Metadados de lineage** presentes em toda tabela Bronze e propagados até
  a Prata: `arquivo_origem`, `data_ingestao`, `timestamp_ingestao`,
  `batch_id`, `hash_linha`, `schema_version`.
- **Datas de negócio vs. datas técnicas**: `dt_transacao` (date, derivada de
  `data_transacao`) é a partição de negócio; nunca confundir com
  `data_ingestao` (quando o dado chegou fisicamente).
- **SCD Tipo 2** (clientes/contas/cartões): `dt_inicio_vigencia`,
  `dt_fim_vigencia` (NULL = versão atual), `flag_vigente`, `versao`,
  `hash_atributos`.

---

## Bronze

| Tabela | Grão | Chave natural | Observações |
|---|---|---|---|
| `bronze_clientes_cdc` | 1 linha por evento de CDC recebido | — (não deduplicada) | Preserva duplicatas e registros inválidos como recebidos |
| `bronze_contas_cdc` | idem | — | |
| `bronze_cartoes_cdc` | idem | — | |
| `bronze_transacoes` | idem | — | `device_id`/`ip_origem` NULL em linhas de schema_version=1 |
| `bronze_eventos_risco` | idem | — | |
| `bronze_estornos` | idem | — | |
| `_ingestion_control` | 1 linha por arquivo processado com sucesso | `(source_name, arquivo_origem, hash_arquivo)` | Controle de idempotência da ingestão |

## Prata

### `silver_clientes` (SCD2)
- **Grão**: 1 linha por versão histórica de `id_cliente`.
- **Chave**: `(id_cliente, versao)`; `id_cliente` + `flag_vigente=true` dá a versão atual.
- **Atributos versionados**: `cpf`, `nome`, `cidade`, `estado`, `renda`, `segmento`.
- **Regras de qualidade**: `estado` ∈ UFs válidas; `renda >= 0`; `cpf` no formato `###.###.###-##`; `nome` não vazio; CPF não pode pertencer a mais de um `id_cliente` (senão ambos vão para quarentena).

### `silver_contas` (SCD2)
- **Grão**: 1 linha por versão histórica de `id_conta`.
- **Atributos versionados**: `id_cliente`, `tipo_conta`, `status_conta` (`ATIVA`/`ENCERRADA`), `data_abertura`.
- **Integridade referencial**: `id_cliente` deve existir em `silver_clientes` (qualquer versão) — senão quarentena (`cliente_inexistente`).

### `silver_cartoes` (SCD2)
- **Grão**: 1 linha por versão histórica de `id_cartao`.
- **Atributos versionados**: `id_conta`, `tipo_cartao`, `limite`, `status_cartao` (`ATIVO`/`CANCELADO`).
- **Integridade referencial**: `id_conta` deve existir em `silver_contas` — senão quarentena (`conta_inexistente`).

### `silver_transacoes`
- **Grão**: 1 linha por `id_transacao` (MERGE idempotente — nunca duplica mesmo recebendo o mesmo id em lotes diferentes).
- **Chave**: `id_transacao`.
- **Partição de negócio**: `dt_transacao` (data, extraída de `data_transacao`) — não é a data de ingestão.
- **Regras de qualidade**: `valor > 0`; `id_cartao` deve existir em `silver_cartoes`; `canal` ∈ {POS, ECOMMERCE, APP, ATM}.

### `silver_eventos_risco`
- **Grão**: 1 linha por `id_evento` (MERGE idempotente).
- **Regras**: `tipo_evento` ∈ {FRAUDE, CHARGEBACK, SUSPEITA}; `severidade` ∈ {BAIXA, MEDIA, ALTA, CRITICA}; `id_transacao` deve existir em `silver_transacoes`.

### `silver_estornos`
- **Grão**: 1 linha por `id_estorno` (MERGE idempotente); **múltiplos estornos por `id_transacao` são válidos** (cenário parcial/complementar).
- **Regras**: `motivo` ∈ {CONTESTACAO_CLIENTE, DUPLICIDADE, ERRO_PROCESSAMENTO, ESTORNO_PARCIAL_COMPLEMENTAR, FRAUDE_CONFIRMADA, REFERENCIA_INVALIDA}; `id_transacao` deve existir.
- **Premissa de negócio** (documentada também em `src/silver/estornos.py`): o layout não traz valor do estorno. Qualquer estorno vinculado zera o `valor_liquido` da transação na Ouro (estorno tratado como total). Em produção, adicionar `valor_estorno`/`percentual_estorno` para suportar estorno parcial real.

### `_quarentena/{entidade}`
- **Grão**: 1 linha por registro reprovado em qualquer regra de qualidade.
- **Colunas extras**: todas as colunas originais + `motivos_quarentena` (texto, motivos concatenados), `entidade`, `batch_id`, `timestamp_quarentena`.

---

## Ouro

### `gold_fato_transacao`
- **Grão**: 1 linha por `id_transacao`.
- **Chaves**: `id_transacao` (PK); `id_cartao`; `id_conta_na_data`, `id_cliente_na_data` (FKs resolvidas **ponto-no-tempo** — quem era o dono na data da transação, não hoje).
- **Colunas de negócio principais**: `valor`, `valor_liquido` (0 quando `flag_estornada=true`), `flag_estornada`, `qtd_estornos`, `status_cartao_na_data`, `flag_cartao_cancelado_no_momento`, `segmento_cliente_na_data`, `cidade_cliente_na_data`, `estado_cliente_na_data`, `flag_evento_risco`, `qtd_eventos_risco`, `severidade_maxima_risco`.
- **Filtros de negócio**: nenhum — grão transacional completo, sem exclusões. Exclusão de cartão cancelado é decisão do consumidor (feita nos agregados).

### `gold_dim_cliente` / `gold_dim_conta` / `gold_dim_cartao`
- **Grão**: 1 linha por chave de negócio (`id_cliente`/`id_conta`/`id_cartao`), sempre a versão **vigente hoje**.
- **Uso**: joins descritivos em BI ("quem é esse cliente"); não usar para reconstruir histórico (usar `silver_*` + join ponto-no-tempo para isso).

### `gold_dim_estabelecimento`
- **Grão**: 1 linha por `(estabelecimento, mcc)`, derivada por agregação de `silver_transacoes` (não existe fonte cadastral).
- **Colunas**: `qtd_transacoes_historico`, `valor_bruto_historico`, `primeira_transacao`, `ultima_transacao`, `qtd_paises_distintos`.

### `gold_cliente_mes`
- **Grão**: 1 linha por `(id_cliente, ano_mes)`.
- **Métricas**: `qtd_transacoes`, `valor_bruto`, `valor_liquido`, `valor_estornado`, `ticket_medio`, `qtd_estabelecimentos_distintos`, `qtd_paises_distintos`, `qtd_transacoes_internacionais`, `qtd_estornos`, `qtd_eventos_risco`, `valor_liquido_mes_anterior` (LAG), `variacao_valor_liquido_pct`.

### `gold_indicadores_risco`
- **Grão**: 1 linha por `(id_cliente, ano_mes)` — mesmo grão de `gold_cliente_mes` para join 1:1.
- **Métricas**: `qtd_eventos_risco`, `qtd_fraude`, `qtd_chargeback`, `qtd_suspeita`, `qtd_estornos`, `valor_estornado`, `taxa_estorno_pct`, `severidade_maxima_mes`.

### `gold_features_cliente`
- **Grão**: 1 linha por `id_cliente` — snapshot mais recente (não histórico).
- **Features**: RFM (`dias_desde_ultima_transacao`, `qtd_transacoes_total`, `valor_liquido_total`), `ticket_medio_total`, `qtd_eventos_risco_total`, `qtd_fraude_total`, `qtd_chargeback_total`, `qtd_estornos_total`, `dias_desde_cadastro`, `percentil_gasto_cidade`, `percentil_gasto_segmento` (PERCENT_RANK), `decil_gasto_geral` (NTILE 10).
- **Consumo**: input direto para modelos de propensão/risco (join por `id_cliente`).
