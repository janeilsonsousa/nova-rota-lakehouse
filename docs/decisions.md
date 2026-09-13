# Decisões técnicas (ADR) — NovaRota Lakehouse

Registro das decisões de arquitetura e engenharia tomadas neste projeto, o
porquê, e os trade-offs considerados. Pensado para sustentar a entrevista de
defesa técnica.

---

## ADR-01 — Ingestão incremental por controle de hash em vez de Auto Loader

**Decisão**: implementar a ingestão Bronze com uma tabela de controle Delta
(`_ingestion_control`) que rastreia `arquivo_origem + hash de conteúdo`,
em vez do Databricks Auto Loader (`cloudFiles`).

**Por quê**: o Auto Loader é a escolha certa em produção para ingestão de
arquivos em object storage cloud (S3/ADLS/GCS), usando directory listing
incremental ou notificação de evento, com checkpoint gerenciado e schema
evolution automático. Neste desafio os arquivos chegam em um diretório
local/Volume, sem um bucket próprio para configurar notificações — replicar
Auto Loader "de verdade" aqui não seria reproduzível por outra pessoa sem
uma conta cloud própria, e é desnecessário para o volume de dados do
desafio.

**O que foi implementado no lugar**: a mesma *propriedade* de idempotência
e incrementalidade — reexecutar não duplica, só processa arquivo novo ou
com conteúdo alterado.

**Como isso vira Auto Loader em produção**: troca mecânica —
`spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation", checkpoint_path).load(raw_path)` com
`trigger(availableNow=True)`; o resto do pipeline (metadados, escrita
Delta) não muda. Ver docstring completo em `src/ingestion/control.py`.

---

## ADR-02 — SCD2 por reconstrução de timeline (delete+insert via MERGE), não MERGE linha-a-linha

**Decisão**: para clientes/contas/cartões, um lote de CDC afetando uma
chave é resolvido recalculando a timeline **inteira** daquela chave
(histórico existente + novas versões, reordenado por `data_atualizacao`
com `LAG`/`ROW_NUMBER`) e substituindo via MERGE INTO (delete das versões
antigas + insert das recalculadas).

**Por quê**: um MERGE INTO tradicional (1 UPDATE de fechamento + 1 INSERT
de abertura por chave) assume **no máximo 1 versão nova por chave por
execução**. Os arquivos de CDC deste desafio não seguem essa premissa — um
único arquivo (`clientes_cdc.csv`) já traz múltiplas versões históricas da
mesma chave. Um MERGE row-a-row nesse cenário falha com "multiple source
rows matched".

**Trade-off aceito**: mais caro computacionalmente (reprocessa a timeline
completa da chave, não só a diferença) — aceitável porque o volume por
chave é pequeno (poucas versões por cliente/conta/cartão). Em um cenário de
altíssimo volume de revisões por chave, valeria a pena revisitar.

**Ganho extra**: dados fora de ordem se resolvem de graça — a timeline é
reordenada do zero a cada execução, então uma versão atrasada reencaixa na
posição cronológica correta, não é apensada no fim.

Ver `src/silver/scd2.py` (docstring completo do módulo).

---

## ADR-03 — Vigência inicial da SCD2 com sentinela, não `data_atualizacao`

**Decisão**: a primeira versão conhecida de cada chave SCD2 recebe
`dt_inicio_vigencia = 1970-01-02` (sentinela "vigente desde sempre"), não o
`data_atualizacao` do evento de CDC que a trouxe.

**Por quê** (bug real encontrado durante o desenvolvimento, via teste
ponta a ponta): `data_atualizacao` é a data em que o sistema de origem
*registrou* aquele estado no CDC, não necessariamente a data em que ele
passou a ser verdade. Um cartão pode ter sido emitido e usado em
transações **antes** do primeiro snapshot de CDC que o sistema enviou. Sem
o sentinela, o join ponto-no-tempo da Ouro (`data_transacao BETWEEN
dt_inicio_vigencia AND dt_fim_vigencia`) deixava **149 de 575 transações
(26%)** sem correspondência de cliente/conta — qualquer transação anterior
ao primeiro `data_atualizacao` do cadastro ficava órfã.

**Por que 1970-01-02, não 1900-01-01** (mais idiomático para "desde
sempre"): timestamp pré-epoch (1970) quebra `datetime.fromtimestamp()` no
Python/Windows ao fazer `collect()` de linhas para o driver — um problema
específico do ambiente de desenvolvimento local (Windows), não de
Databricks (Linux), mas que preferimos evitar para manter os testes
reproduzíveis nos dois sistemas operacionais.

---

## ADR-04 — Estorno sem valor: premissa de estorno total

**Decisão**: uma transação é tratada como totalmente estornada
(`valor_liquido = 0`) se tiver **pelo menos 1** registro de estorno válido
vinculado, mesmo quando existem múltiplos estornos para a mesma
`id_transacao` (cenário parcial/complementar simulado na massa).

**Por quê**: o layout mínimo do desafio não inclui `valor_estorno` nem
`percentual_estorno` (documentado no próprio `README_DADOS.md` da massa
sintética). Sem esse campo, não há como calcular estorno parcial real.

**Em produção**: adicionar `valor_estorno` ou `percentual_estorno` ao
contrato de dados de origem, e mudar `valor_liquido` para
`valor - SUM(valor_estorno)` por transação.

---

## ADR-05 — Gold não filtra cartão cancelado na fato; filtra nos agregados

**Decisão**: `gold_fato_transacao` inclui transações de cartões hoje
cancelados normalmente (com `flag_cartao_cancelado_no_momento` calculada
ponto-no-tempo). A exclusão de cartões cancelados de "métricas futuras"
acontece nos agregados (`gold_cliente_mes`), não na fato.

**Por quê**: o cancelamento pode ter acontecido **depois** da transação —
a transação era legítima quando ocorreu. Filtrar na fato destruiria
histórico. A regra de negócio "cartão cancelado não compõe métricas
futuras" é sobre o que fazer com o cartão **hoje**, não sobre apagar o
passado — por isso vive na camada de consumo/agregação, que pode decidir
por período.

---

## ADR-06 — Agregados mensais recomputados por chave composta afetada, não incrementados

**Decisão**: `gold_cliente_mes` e `gold_indicadores_risco` recalculam o
grupo inteiro `(id_cliente, ano_mes)` quando qualquer transação daquele par
muda (mesmo padrão de "reconstrução por chave afetada" do ADR-02),
substituindo via MERGE INTO (delete+insert), em vez de somar
incrementalmente ao agregado existente.

**Por quê**: métricas não-aditivas (ticket médio, contagem distinta de
estabelecimentos, `LAG` do mês anterior) não podem ser corrigidas com um
UPDATE incremental (`agregado += novo_valor`) quando um dado atrasado
chega para um mês já fechado — o dado atrasado do requisito 9 ("arquivos
podem chegar fora de ordem") exige reagregação completa do grupo.

**Custo aceito**: reagregar um mês inteiro é mais caro que somar 1 linha,
mas o volume por (cliente, mês) é pequeno; a alternativa (manter agregados
parciais + fórmulas de correção incremental para cada métrica não-aditiva)
é significativamente mais complexa e frágil.

---

## ADR-07 — Escopo incremental da Ouro via `batch_id`, não `data_referencia`

**Decisão**: `gold_fato_transacao` processa por padrão apenas as
transações cujo `batch_id` (Prata) é igual ao `batch_id` da execução atual
do pipeline — o mesmo identificador atravessa Bronze → Prata → Ouro.
`run_mode=full` ignora esse filtro e reprocessa tudo (backfill/correção
retroativa).

**Por quê**: como Bronze e Prata já usam `batch_id` para escopo
incremental, manter o mesmo identificador na Ouro garante que "o que a
Prata acabou de tocar" seja exatamente "o que a Ouro vai reconsolidar" —
sem depender de uma segunda tabela de controle ou de comparar timestamps
entre camadas. `data_referencia` continua disponível na configuração para
cenários de reprocessamento manual direcionado (ex.: rodar
`run_mode=full` e depois filtrar/auditar por mês via SQL).

---

## ADR-08 — Ambiente de desenvolvimento local no Windows

Vários bugs específicos do ambiente Windows local (não de Databricks/Spark
em si) foram encontrados e corrigidos durante o desenvolvimento. Registrados
aqui porque consumiram tempo real de engenharia e porque a causa raiz não é
óbvia:

1. **PySpark resolvendo o Python errado**: os workers do PySpark local
   podem resolver `python` pelo PATH do sistema operacional em vez do
   interpretador do venv — no Windows isso pode acidentalmente pegar o
   alias da Microsoft Store, quebrando a serialização com um erro de
   socket (`Connection reset`) difícil de diagnosticar. Corrigido fixando
   `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` explicitamente para
   `sys.executable` (`src/utils/spark_session.py`).

2. **Falta de `winutils.exe`**: Spark no Windows precisa do `winutils`
   para operações de filesystem usadas pelo Delta Lake. Resolvido
   vendorizando um binário mínimo em `tools/hadoop-win/` e detectando-o
   automaticamente — qualquer pessoa clonando o repositório no Windows
   roda sem setup manual.

3. **`spark.createDataFrame(lista_python, schema)` instável**: neste
   ambiente específico (PySpark 3.5.1 + Python 3.12 + Windows), qualquer
   ação que precise mover dados reais do driver para o worker Python via
   pickle/RDD (`createDataFrame` a partir de uma lista Python,
   `df.rdd.isEmpty()`) apresentava falhas intermitentes
   (`EOFException`/worker "crashed"). **Não é um problema de lógica de
   negócio** — operações 100% JVM/Catalyst (leitura de CSV, `count()`,
   `write`, SQL com funções built-in) sempre funcionaram normalmente.
   Contornado com `build_literal_df` (`src/utils/spark_helpers.py`), que
   constrói DataFrames pequenos via `spark.range(1).select(lit(...))` —
   uma operação 100% JVM — e evitando `.rdd.isEmpty()` em favor de
   `.limit(1).count()`.

**Por que registrar isso**: em Databricks (cluster Linux gerenciado) nada
disso se aplica — é puramente uma questão de rodar/testar localmente antes
de subir para o workspace. Preferimos investigar e corrigir a causa raiz
(permitindo testes locais reais e rápidos) a mascarar o sintoma ou pular a
validação local.

---

## O que ficou como desenho (não implementado) e por quê

- **Auto Loader real / Structured Streaming**: ver ADR-01 — desenho
  documentado, não implementado por falta de object storage cloud
  configurável de forma reproduzível para este desafio.
- **Testes de carga/volume real**: a massa sintética é pequena por design
  (demonstração de lógica, não de escala). Estratégia de performance para
  volume real está em `docs/architecture.md` (particionamento, OPTIMIZE,
  Z-ORDER, Liquid Clustering).
- **CDC com operação `D` (delete físico)**: a massa só contém `I`/`U`. O
  código de qualidade/SCD2 não trata delete explícito — em produção,
  adicionar tratamento de `operacao = 'D'` fecharia a versão vigente
  (`flag_vigente=false`) sem abrir uma nova.
- **Unity Catalog / permissões reais**: desenhado em
  `docs/architecture.md` (seção de governança), não provisionado (exige
  workspace Databricks com admin de conta).
