# Contratos de dados

Grão, chaves e regras de negócio de cada tabela — referência pra quem consumir a Ouro ou mexer na Prata.

## Convenções gerais

- Nome da tabela: `{camada}_{entidade}` (ex. `silver_clientes`, `gold_fato_transacao`).
- Lineage em toda tabela Bronze, propagado até a Prata: `arquivo_origem`, `data_ingestao`, `timestamp_ingestao`, `batch_id`, `hash_linha`, `schema_version`.
- `dt_transacao` é a partição de negócio (data em que a transação aconteceu), não confundir com `data_ingestao` (quando o dado chegou).
- SCD2 (clientes/contas/cartões) usa sempre `dt_inicio_vigencia`, `dt_fim_vigencia` (NULL = vigente), `flag_vigente`, `versao`, `hash_atributos`.

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
| `_ingestion_control` | 1 linha por arquivo processado com sucesso | `(source_name, arquivo_origem, hash_arquivo)` | controle de idempotência da ingestão |

## Prata

| Tabela | Grão / chave | Atributos versionados / regras |
|---|---|---|
| `silver_clientes` (SCD2) | 1 linha por versão de `id_cliente`; `(id_cliente, versao)`, `flag_vigente=true` = atual | `cpf`, `nome`, `cidade`, `estado`, `renda`, `segmento`. Regras: estado ∈ UFs válidas, renda ≥ 0, cpf no formato `###.###.###-##`, nome não vazio, CPF duplicado entre clientes distintos vai pra quarentena |
| `silver_contas` (SCD2) | 1 linha por versão de `id_conta` | `id_cliente`, `tipo_conta`, `status_conta` (ATIVA/ENCERRADA), `data_abertura`. `id_cliente` tem que existir em `silver_clientes`, senão quarentena (`cliente_inexistente`) |
| `silver_cartoes` (SCD2) | 1 linha por versão de `id_cartao` | `id_conta`, `tipo_cartao`, `limite`, `status_cartao` (ATIVO/CANCELADO). `id_conta` tem que existir em `silver_contas`, senão quarentena (`conta_inexistente`) |
| `silver_transacoes` | 1 linha por `id_transacao`, MERGE idempotente | `dt_transacao` é a partição de negócio. Regras: valor > 0, `id_cartao` existente, canal ∈ {POS, ECOMMERCE, APP, ATM} |
| `silver_eventos_risco` | 1 linha por `id_evento`, MERGE idempotente | tipo_evento ∈ {FRAUDE, CHARGEBACK, SUSPEITA}, severidade ∈ {BAIXA, MEDIA, ALTA, CRITICA}, `id_transacao` existente |
| `silver_estornos` | 1 linha por `id_estorno`, MERGE idempotente; mais de um estorno pra mesma transação é válido | motivo ∈ {CONTESTACAO_CLIENTE, DUPLICIDADE, ERRO_PROCESSAMENTO, ESTORNO_PARCIAL_COMPLEMENTAR, FRAUDE_CONFIRMADA, REFERENCIA_INVALIDA}. Layout não traz valor do estorno, então qualquer estorno zera o `valor_liquido` na Ouro (ver `src/silver/estornos.py`) |
| `_quarentena/{entidade}` | 1 linha por registro reprovado em alguma regra | colunas originais + `motivos_quarentena`, `entidade`, `batch_id`, `timestamp_quarentena` |

## Ouro

| Tabela | Grão / chave | Colunas principais |
|---|---|---|
| `gold_fato_transacao` | 1 linha por `id_transacao` | `id_cartao`, `id_conta_na_data`/`id_cliente_na_data` (FK ponto-no-tempo, dono na data da transação), `valor`, `valor_liquido`, `flag_estornada`, `status_cartao_na_data`, `flag_cartao_cancelado_no_momento`, dados do cliente na data, flags de risco. Nada é filtrado aqui, nem cartão cancelado |
| `gold_dim_cliente` / `gold_dim_conta` / `gold_dim_cartao` | 1 linha por chave de negócio, sempre vigente hoje | uso em BI ("quem é esse cliente"); pra histórico usar `silver_*` + join ponto-no-tempo |
| `gold_dim_estabelecimento` | 1 linha por `(estabelecimento, mcc)`, derivada de `silver_transacoes` (não tem cadastro) | `qtd_transacoes_historico`, `valor_bruto_historico`, `primeira_transacao`, `ultima_transacao`, `qtd_paises_distintos` |
| `gold_cliente_mes` | 1 linha por `(id_cliente, ano_mes)` | `qtd_transacoes`, `valor_bruto`, `valor_liquido`, `valor_estornado`, `ticket_medio`, distintos de estabelecimento/país, transações internacionais, `valor_liquido_mes_anterior` (LAG), variação % |
| `gold_indicadores_risco` | 1 linha por `(id_cliente, ano_mes)`, mesmo grão de `gold_cliente_mes` pra join 1:1 | `qtd_eventos_risco`, `qtd_fraude`, `qtd_chargeback`, `qtd_suspeita`, `qtd_estornos`, `valor_estornado`, `taxa_estorno_pct`, `severidade_maxima_mes` |
| `gold_features_cliente` | 1 linha por `id_cliente`, snapshot mais recente (não histórico) | RFM (dias desde última transação, total de transações, valor total), ticket médio, totais de risco/fraude/chargeback/estorno, dias desde cadastro, percentil por cidade/segmento, decil geral. Input direto pra modelo de propensão/risco |
