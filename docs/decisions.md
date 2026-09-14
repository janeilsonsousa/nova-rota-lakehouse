# Decisões técnicas

Registro rápido das decisões de arquitetura tomadas no projeto e por quê, pra não esquecer o raciocínio depois.

---

## ADR-01 — Ingestão por hash em vez de Auto Loader

Usei uma tabela de controle Delta (`_ingestion_control`) que guarda `arquivo_origem + hash do conteúdo`, em vez do Auto Loader (`cloudFiles`) do Databricks.

Auto Loader é a escolha certa em produção com object storage cloud (S3/ADLS/GCS) — directory listing incremental ou notificação de evento, checkpoint gerenciado, schema evolution automático. Aqui os arquivos chegam num diretório local/Volume, sem bucket próprio pra configurar notificação, então não dava pra reproduzir de verdade sem uma conta cloud.

O que ficou: a mesma propriedade de idempotência — reexecutar não duplica, só processa arquivo novo ou com conteúdo mudado.

Trocar por Auto Loader em produção é mecânico: `spark.readStream.format("cloudFiles")...` com `trigger(availableNow=True)`, o resto do pipeline não muda. Ver `src/ingestion/control.py`.

---

## ADR-02 — SCD2 recalcula a timeline inteira em vez de MERGE linha-a-linha

Um lote de CDC que afeta uma chave (cliente/conta/cartão) recalcula a timeline inteira daquela chave (histórico + novas versões, reordenado por `data_atualizacao`) e substitui via MERGE (delete+insert).

Motivo: um MERGE tradicional assume no máximo 1 versão nova por chave por execução. Os arquivos de CDC não seguem essa regra — um `clientes_cdc.csv` pode trazer 2 versões do mesmo cliente no mesmo arquivo. MERGE linha-a-linha quebra com "multiple source rows matched" nesse caso.

Custo: reprocessa a timeline completa da chave, não só a diferença — ok porque o volume por chave é pequeno. Ganho de brinde: dado fora de ordem se resolve sozinho, porque a timeline é reordenada do zero toda vez.

Ver `src/silver/scd2.py`.

---

## ADR-03 — Sentinela de vigência inicial, não `data_atualizacao`

A primeira versão de cada chave SCD2 recebe `dt_inicio_vigencia = 1970-01-02`, não o `data_atualizacao` do evento CDC que trouxe ela.

Bug real que encontrei testando ponta a ponta: `data_atualizacao` é a data em que o CDC *registrou* aquele estado, não a data em que ele passou a ser verdade. Um cartão pode já existir e ter transações antes do primeiro snapshot de CDC recebido. Sem o sentinela, o join ponto-no-tempo da Ouro deixava 149 de 575 transações (26%) sem cliente/conta correspondente.

1970-01-02 e não 1900: timestamp pré-epoch quebra `datetime.fromtimestamp()` no Windows ao fazer `collect()` — problema só do dev local, mas evita dor de cabeça nos dois SOs.

---

## ADR-04 — Estorno tratado como total

Uma transação com pelo menos 1 estorno vinculado vira `valor_liquido = 0`, mesmo se tiver mais de um registro de estorno pra mesma transação.

O layout não traz `valor_estorno` nem `percentual_estorno` (ver `README_DADOS.md` da massa), então não tem como calcular parcial de verdade. Em produção: adicionar esse campo e trocar pra `valor - SUM(valor_estorno)`.

---

## ADR-05 — Cartão cancelado não é filtrado na fato

`gold_fato_transacao` mantém transações de cartão hoje cancelado normalmente (com a flag `flag_cartao_cancelado_no_momento`). O filtro de "não conta em métrica futura" fica nos agregados (`gold_cliente_mes`), não na fato.

O cancelamento pode ter vindo depois da transação — a transação era válida quando aconteceu. Filtrar na fato apagaria histórico real.

---

## ADR-06 — Agregados mensais recalculam o grupo inteiro, não somam incremental

`gold_cliente_mes` e `gold_indicadores_risco` recalculam `(id_cliente, ano_mes)` inteiro quando qualquer transação do par muda, e substituem via MERGE (mesmo padrão do ADR-02).

Ticket médio, contagem distinta, LAG do mês anterior — nenhuma dessas é aditiva, então um UPDATE incremental não corrige quando chega dado atrasado pra um mês já fechado. Reagregar o mês inteiro é mais caro que somar uma linha, mas o volume por (cliente, mês) é pequeno.

---

## ADR-07 — Ouro usa `batch_id`, não `data_referencia`, pro escopo incremental

`gold_fato_transacao` processa por padrão só as transações com o `batch_id` da execução atual — o mesmo id atravessa Bronze → Prata → Ouro. `run_mode=full` ignora isso e reprocessa tudo.

Como Bronze e Prata já usam `batch_id`, manter o mesmo id na Ouro evita precisar de uma segunda tabela de controle. `data_referencia` fica disponível pra reprocessamento manual direcionado.

---

## ADR-08 — Bugs do Windows local

Alguns bugs específicos do ambiente Windows (não do Spark/Databricks em si), registrados porque consumiram tempo real e a causa não é óbvia:

1. **PySpark resolvendo o Python errado**: os workers podem pegar `python` do PATH do sistema em vez do venv — no Windows isso às vezes pega o alias da Microsoft Store e quebra com `Connection reset`. Corrigido fixando `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` pro `sys.executable` (`src/utils/spark_session.py`).
2. **Falta `winutils.exe`**: Spark no Windows precisa dele pro Delta Lake. Vendorizado em `tools/hadoop-win/` e detectado sozinho.
3. **`spark.createDataFrame(lista, schema)` instável**: nesse ambiente (PySpark 3.5.1 + Python 3.12 + Windows), qualquer coisa que mova dado via pickle/RDD (`createDataFrame` de lista, `.rdd.isEmpty()`) falhava intermitente com `EOFException`. Operação 100% JVM (CSV, `count()`, `write`, SQL) sempre funcionou. Contornado com `build_literal_df` (`src/utils/spark_helpers.py`) e `.limit(1).count()` em vez de `.rdd.isEmpty()`.

Em Databricks (Linux gerenciado) nada disso se aplica.

---

## ADR-09 — Databricks Serverless e registro no Unity Catalog

A evidência real (`docs/evidencias/`) rodou em compute Serverless de um workspace Free Edition — sem custo, sem gerenciar cluster. Serverless tem isolamento mais restrito, precisou de 3 ajustes (nenhum de lógica de negócio):

1. **Sem `SparkContext`/RDD** (`JVM_ATTRIBUTE_NOT_SUPPORTED`) — resolvido com `build_literal_df`, 100% DataFrame API.
2. **`.persist()` não suportado** — removido, é otimização, não afeta corretude nesse volume.
3. **Workspace filesystem é read-only pra dado** — storage físico virou um Volume Unity Catalog, criado no notebook `00_setup_ambiente`.

O pipeline lê/escreve por path físico (portável entre local e Databricks) e no final de cada camada tenta registrar a tabela no Unity Catalog por nome. Volume não é aceito como `LOCATION` de tabela registrada, então a função tenta `CREATE TABLE ... LOCATION` e cai pra `CREATE OR REPLACE TABLE ... AS SELECT` (managed table, com cópia física — desprezível nesse volume). É best-effort: falha nisso não derruba o pipeline. Ver `src/utils/catalog.py` e as evidências das 13 tabelas em `docs/evidencias/`.

---

## O que ficou de fora

- **Auto Loader / Structured Streaming real**: desenhado no ADR-01, não implementado por falta de object storage cloud reproduzível.
- **Teste de carga/volume**: massa sintética é pequena de propósito. Estratégia de performance pra volume real está em `docs/architecture.md`.
- **CDC com delete físico (`operacao='D'`)**: a massa só tem `I`/`U`. Em produção, tratar `D` fecharia a versão vigente sem abrir nova.
- **Unity Catalog com permissão granular por camada**: catálogo e tabelas estão registrados, mas segregação de permissão por papel exige workspace corporativo com admin de conta — desenho em `docs/architecture.md`.
