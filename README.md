# NovaRota Lakehouse

Projeto de Engenharia de Dados desenvolvido como solução para um desafio técnico de nível Sênior, com foco em **Databricks, Delta Lake, PySpark, arquitetura Medallion, qualidade de dados, processamento incremental, histórico SCD Tipo 2, SQL analítico e boas práticas de engenharia de software**.

A solução implementa um pipeline ponta a ponta para dados transacionais da Cooperativa NovaRota, organizando o processamento nas camadas **Bronze → Prata → Ouro** e mantendo o código principal fora dos notebooks, em módulos Python reutilizáveis e testáveis.

## Visão geral

O cenário simula uma cooperativa com dados de clientes, contas, cartões, transações, eventos de risco e estornos. A massa de dados foi criada de forma sintética e contém cenários de qualidade e processamento comuns em ambientes reais, como registros duplicados, dados inválidos, mudanças cadastrais, arquivos fora de ordem, evolução de schema e referências inexistentes.

O pipeline foi construído para ser executado tanto localmente, com Spark + Delta Lake, quanto no Databricks.

### Principais entregas

- Arquitetura Medallion com camadas Bronze, Prata e Ouro.
- Ingestão incremental e idempotente de arquivos.
- Evolução de schema na camada Bronze.
- Regras de qualidade centralizadas.
- Quarentena de registros inválidos com motivo da rejeição.
- SCD Tipo 2 para clientes, contas e cartões.
- MERGE incremental para entidades transacionais.
- Tratamento de arquivos atrasados e dados fora de ordem nas entidades historizadas.
- Join entre transações e dimensões historizadas.
- Camada Ouro com fato, dimensões, indicadores mensais, risco e features para Data Science.
- SQL analítico com funções de janela e consultas de comportamento.
- Logs estruturados com identificação do lote.
- Testes automatizados de configuração e transformação.
- Execução validada em Databricks Serverless com Unity Catalog.
- Evidências de execução, testes e volumetria versionadas no projeto.

---

## Arquitetura

```text
Arquivos CSV / CDC
        |
        v
+------------------------------+
|            BRONZE            |
|------------------------------|
| Dados brutos                 |
| Metadados de ingestão        |
| Controle de arquivos         |
| Evolução de schema           |
+------------------------------+
        |
        v
+------------------------------+
|            SILVER             |
|------------------------------|
| Tipagem e limpeza            |
| Regras de qualidade          |
| Quarentena                   |
| SCD Tipo 2                   |
| MERGE incremental            |
| Integridade referencial      |
+------------------------------+
        |
        v
+------------------------------+
|             OURO             |
|------------------------------|
| Fato transacional            |
| Dimensões                    |
| Métricas mensais             |
| Indicadores de risco         |  |
+------------------------------+
```

A implementação principal fica no pacote `src/`. Os notebooks são utilizados apenas como camada de execução e demonstração no Databricks.

---

## Fontes de dados

A massa de entrada está em `data/raw/nova_rota_input/`.

| Fonte | Finalidade |
|---|---|
| `clientes_cdc.csv` | Cadastro e alterações de clientes |
| `contas_cdc.csv` | Contas vinculadas aos clientes |
| `cartoes_cdc.csv` | Cartões e mudanças de status/limite |
| `transacoes/*.csv` | Eventos transacionais particionados em arquivos |
| `eventos_risco.csv` | Fraude, chargeback e suspeitas |
| `estornos.csv` | Estornos vinculados às transações |

A massa inclui intencionalmente cenários de duplicidade, inconsistência, atualização cadastral, atraso de arquivo e evolução de schema para validar o comportamento do pipeline.

Mais detalhes em [`data/raw/nova_rota_input/README_DADOS.md`](data/raw/nova_rota_input/README_DADOS.md).

---

## Camada Bronze

A Bronze recebe os arquivos sem aplicar regras de negócio. O objetivo é preservar o conteúdo recebido e adicionar informações de rastreabilidade.

As principais responsabilidades são:

- leitura dos arquivos de origem;
- persistência em Delta Lake;
- controle de arquivos já processados;
- ingestão incremental;
- identificação do lote por `batch_id`;
- registro do arquivo de origem;
- data e timestamp de ingestão;
- hash da linha;
- controle de versão do schema.

Tabelas principais:

```text
bronze_clientes_cdc
bronze_contas_cdc
bronze_cartoes_cdc
bronze_transacoes
bronze_eventos_risco
bronze_estornos
```

Também é mantida uma estrutura de controle de ingestão para impedir que um mesmo arquivo seja processado novamente de forma indevida.

### Idempotência da Bronze

Cada arquivo é identificado pelo nome e pelo hash do conteúdo. Em execução incremental, um arquivo já processado com o mesmo conteúdo não é ingerido novamente.

O modo `full` permite reprocessamento completo quando necessário.

---

## Camada Prata

A Prata concentra tipagem, limpeza, regras de qualidade, integridade referencial e histórico das entidades.

### Clientes, contas e cartões

As três entidades utilizam **SCD Tipo 2**, mantendo as diferentes versões ao longo do tempo.

Campos de controle do histórico:

```text
dt_inicio_vigencia
dt_fim_vigencia
flag_vigente
versao
hash_atributos
```

Quando uma chave é afetada por um novo lote, sua timeline é reconstruída e ordenada por `data_atualizacao`. Isso permite tratar mais de uma alteração da mesma chave no mesmo arquivo e também arquivos recebidos fora de ordem.

### Transações

As transações são consolidadas por `id_transacao`, utilizando MERGE para manter uma única versão lógica da transação na Prata.

Entre as validações estão:

- valor positivo;
- cartão existente;
- domínio válido de canal;
- tratamento de duplicidade.

### Eventos de risco e estornos

Eventos e estornos também utilizam chaves naturais e processamento incremental.

Os eventos de risco validam, entre outros pontos:

- tipo do evento;
- severidade;
- existência da transação referenciada.

Os estornos preservam os vínculos com as transações. Como o layout de origem não possui `valor_estorno`, a solução considera o estorno como total na consolidação da Ouro.

---

## Qualidade de dados e quarentena

As regras de qualidade ficam centralizadas em `src/quality/`.

Um registro reprovado não é descartado silenciosamente. Ele é direcionado para a quarentena junto com o motivo da rejeição e os metadados da execução.

Exemplos de validação:

```text
estado inválido
renda negativa
CPF em formato inválido
cliente inexistente
conta inexistente
cartão inexistente
valor de transação inválido
canal inválido
referência de transação inexistente
tipo de evento de risco inválido
severidade inválida
```

A abordagem permite rastrear o motivo da rejeição sem interromper o processamento dos registros válidos do lote.

---

## Camada Ouro

A Ouro disponibiliza os dados preparados para consumo analítico e Data Science.

### `gold_fato_transacao`

Grão:

```text
1 linha por id_transacao
```

A fato consolida dados da transação com cliente, conta e cartão válidos **na data em que a transação aconteceu**.

O vínculo histórico é resolvido por join ponto-no-tempo entre a data da transação e o intervalo de vigência das dimensões SCD2.

Também são adicionados indicadores de estorno, eventos de risco e situação do cartão.

A fato preserva as transações para fins de histórico e auditoria. O status do cartão no momento da transação é materializado em coluna própria para permitir aplicação das regras de consumo sem perda do evento histórico.

### Dimensões

Foram construídas dimensões para:

```text
gold_dim_cliente
gold_dim_conta
gold_dim_cartao
gold_dim_estabelecimento
```

As dimensões de cliente, conta e cartão representam a visão vigente da entidade. Para análises históricas, a fonte de referência permanece a Silver com SCD2.

### `gold_cliente_mes`

Grão:

```text
id_cliente + ano_mes
```

Contém métricas mensais como:

- quantidade de transações;
- valor bruto;
- valor líquido;
- valor estornado;
- ticket médio;
- estabelecimentos distintos;
- países distintos;
- transações internacionais;
- quantidade de estornos;
- eventos de risco;
- valor líquido do mês anterior;
- variação mensal.

Os pares `(cliente, mês)` afetados são recalculados por completo. Essa decisão evita erros em métricas não aditivas, como ticket médio, contagem distinta e comparação com o mês anterior.

### `gold_indicadores_risco`

Consolida indicadores mensais relacionados a:

- fraude;
- chargeback;
- suspeitas;
- estornos;
- severidade máxima;
- taxa de estorno.

### `gold_features_cliente`

Tabela preparada para consumo por Data Science, com features comportamentais derivadas do histórico transacional.

Entre elas:

- recência;
- frequência;
- valor movimentado;
- ticket médio;
- indicadores de risco;
- percentis por cidade/segmento;
- segmentação em decis.

---

## SQL analítico

As consultas solicitadas no desafio estão em:

[`sql/advanced_queries.sql`]

O arquivo demonstra uso de:

```text
CTEs encadeadas
ROW_NUMBER
LAG
LEAD
FIRST_VALUE
LAST_VALUE
NTILE
PERCENT_RANK
MERGE INTO
```

Também inclui consultas para análise de comportamento, comparação do cliente contra seu próprio histórico, comparação por cidade/segmento e identificação de anomalias simples.

---

## Estrutura do projeto

```text
nova-rota-lakehouse/
|
|-- .github/
|   `-- workflows/
|       `-- ci.yml
|
|-- data/
|   `-- raw/
|       `-- nova_rota_input/
|
|-- docs/
|   |-- architecture.md
|   |-- decisions.md
|   |-- data_contracts.md
|   `-- evidencias/
|
|-- notebooks/
|   |-- 00_setup_ambiente.py
|   |-- 01_bronze_ingestion.py
|   |-- 02_silver_processing.py
|   |-- 03_gold_data_products.py
|   `-- 04_sql_analysis.py
|
|-- sql/
|   `-- advanced_queries.sql
|
|-- src/
|   |-- config/
|   |-- gold/
|   |-- ingestion/
|   |-- quality/
|   |-- silver/
|   |-- utils/
|   `-- run_pipeline.py
|
|-- tests/
|   |-- transform/
|   `-- unit/
|
|-- .gitignore
|-- pyproject.toml
|-- requirements.txt
`-- README.md
```

### Organização do código

| Diretório | Responsabilidade |
|---|---|
| `src/config` | Configuração central do pipeline |
| `src/ingestion` | Ingestão e controle da Bronze |
| `src/quality` | Regras reutilizáveis de qualidade |
| `src/silver` | Transformações, SCD2 e MERGEs da Prata |
| `src/gold` | Fato, dimensões e data products |
| `src/utils` | Spark, catálogo, logging e helpers |
| `tests` | Testes automatizados |
| `sql` | Consultas analíticas |
| `notebooks` | Execução e demonstração no Databricks |
| `docs` | Arquitetura, decisões, contratos e evidências |

---

## Tecnologias utilizadas

- Python
- PySpark
- Delta Lake / `delta-spark`
- Databricks
- Unity Catalog
- Spark SQL
- Pytest
- Ruff
- Git / GitHub
- GitHub Actions

---

## Configuração central

Os parâmetros ficam em:

```text
src/config/settings.py
```

A configuração pode ser sobrescrita por argumentos da CLI ou por variáveis de ambiente com prefixo `NOVAROTA_`.

Principais parâmetros:

| Parâmetro | Default | Descrição |
|---|---|---|
| `env` | `local` | Ambiente de execução |
| `catalog` | `nova_rota` | Catálogo no Databricks |
| `bronze_schema` | `bronze` | Schema da Bronze |
| `silver_schema` | `silver` | Schema da Prata |
| `gold_schema` | `gold` | Schema da Ouro |
| `base_path` | `data/lakehouse` | Raiz das tabelas Delta |
| `raw_path` | `data/raw/nova_rota_input` | Origem dos arquivos |
| `data_referencia` | data atual | Data de referência disponibilizada para execução/reprocessamento |
| `run_mode` | `incremental` | `incremental` ou `full` |
| `batch_id` | UUID | Identificador do lote |

No fluxo incremental atual, o `batch_id` é o principal identificador propagado entre Bronze, Prata e Ouro. A `data_referencia` permanece disponível como parâmetro operacional para execuções direcionadas e evolução do desenho de reprocessamento.

---

## Pré-requisitos

Para execução local:

```text
Python
```

O projeto também foi validado em Databricks Serverless.

---

## Setup local

Criar o ambiente virtual:

```bash
python -m venv .venv
```

### Windows

```bash
.venv\Scripts\activate
pip install -r requirements.txt
```

### Linux/macOS

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

No Windows, o projeto possui suporte ao `winutils.exe` utilizado pelo Spark local e configuração específica em `src/utils/spark_session.py`.

---

## Executando o pipeline

Execução completa local:

```bash
python -m src.run_pipeline --env local --run-mode incremental
```

A execução segue a ordem:

```text
Bronze -> Prata -> Ouro
```

Para consultar todas as opções:

```bash
python -m src.run_pipeline --help
```

### Executar somente algumas camadas

Somente Bronze:

```bash
python -m src.run_pipeline --env local --layers bronze
```

Bronze e Prata:

```bash
python -m src.run_pipeline --env local --layers bronze,silver
```

Somente Ouro:

```bash
python -m src.run_pipeline --env local --layers gold
```

### Execução full

```bash
python -m src.run_pipeline --env local --run-mode full
```

### Batch controlado

```bash
python -m src.run_pipeline \
  --env local \
  --run-mode incremental \
  --batch-id teste-001
```

### Data de referência

```bash
python -m src.run_pipeline \
  --env local \
  --data-referencia 2026-04-10
```

---

## Execução no Databricks

Os notebooks disponíveis são:

```text
00_setup_ambiente
01_bronze_ingestion
02_silver_processing
03_gold_data_products
04_sql_analysis
```

A ordem recomendada é:

```text
00 -> 01 -> 02 -> 03 -> 04
```

O notebook `00_setup_ambiente` prepara o ambiente necessário para execução no Databricks e os demais notebooks acionam o código modular existente em `src/`.

A solução foi validada em Databricks Serverless e possui ajustes de compatibilidade documentados em [`docs/decisions.md`](docs/decisions.md).

No Databricks, as tabelas são registradas no Unity Catalog sempre que o ambiente permite.

---

## Testes automatizados

A suíte completa possui **28 testes automatizados**.

Resultado registrado:

```text
28 passed in 319.04s
```

A evidência está em:

[`docs/evidencias/pytest_full_suite.log`](docs/evidencias/pytest_full_suite.log)

### Executar toda a suíte

```bash
python -m pytest -q
```

### Testes focados em configuração/lógica

```bash
python -m pytest tests/unit -q
```

### Testes de transformação

```bash
python -m pytest tests/transform -q
```

Os testes de transformação utilizam Spark com massa pequena e validam regras das camadas Bronze, Prata e Ouro.

---

## CI

O projeto possui workflow em:

```text
.github/workflows/ci.yml
```

O CI executa em pushes e pull requests para `main` e `develop`.

Etapas:

```text
1. lint com Ruff
2. testes de configuração/lógica
3. testes de transformação com Spark
```

Os testes de transformação utilizam Java 17 no runner.

---

## Logs e observabilidade

O pipeline utiliza logs estruturados e propaga o `batch_id` durante a execução.

Cada camada registra informações como início da etapa, quantidade de registros processados, registros rejeitados e conclusão.

Falhas críticas não são mascaradas: exceções são propagadas para que o processo de orquestração consiga identificar corretamente uma execução com erro.

Em produção, os logs e indicadores de qualidade poderiam alimentar alertas de falha, SLA, volumetria e taxa de registros enviados para quarentena.

---

## Idempotência

A idempotência é tratada em diferentes níveis:

**Bronze**

```text
controle de arquivo + hash do conteúdo
```

**Prata**

```text
SCD2 para entidades historizadas
MERGE por chave natural nas entidades transacionais
```

**Ouro**

```text
MERGE da fato por id_transacao
reprocessamento dos grupos analíticos afetados
```

Assim, reexecutar um mesmo lote não deve gerar duplicação lógica dos dados.

---

## Performance

Para a massa do desafio, a prioridade foi corretude e clareza do fluxo. O volume é pequeno e, por isso, não foram criadas partições artificiais que poderiam gerar excesso de arquivos pequenos.

Para produção, a estratégia considerada inclui:

- Liquid Clustering ou Z-ORDER na fato transacional;
- organização por `dt_transacao` e `id_cliente_na_data` de acordo com padrão de consulta;
- `OPTIMIZE` periódico;
- `optimizeWrite` e `autoCompact`;
- broadcast de dimensões pequenas;
- monitoramento de arquivos pequenos e crescimento de volume.

A estratégia completa está documentada em [`docs/architecture.md`](docs/architecture.md).

---

## Governança

O desenho considera o catálogo `nova_rota` com separação por schemas:

```text
nova_rota.bronze
nova_rota.silver
nova_rota.gold
```

Modelo de acesso proposto para produção:

| Camada | Acesso esperado |
|---|---|
| Bronze | Engenharia de Dados |
| Prata | Engenharia + leitura controlada por analistas |
| Ouro | BI, Analytics e Data Science |

A escrita das três camadas deve permanecer restrita ao pipeline.

---

## Estratégia de Git

O desenvolvimento foi dividido por domínio funcional, permitindo acompanhar a evolução do projeto.

Branches utilizadas ao longo da implementação incluem:

```text
main
develop
feature/ingestion-framework
feature/dq-and-scd
feature/gold-data-products
feature/docs-and-notebooks
feature/databricks-execution-fixes
feature/catalog-registration-evidence
```

Os commits seguem um padrão semelhante a Conventional Commits:

```text
feat:
fix:
refactor:
docs:
chore:
merge:
```

A divisão permite visualizar a construção incremental da solução, desde a estrutura inicial e Bronze até qualidade/SCD2, Ouro, documentação e validação real no Databricks.

---

## Evidências

As evidências ficam em:

[`docs/evidencias/`](docs/evidencias/)

Foram armazenados:

- log de execução local ponta a ponta;
- evidência de idempotência;
- resultado da suíte com 28 testes;
- prints da execução no Databricks;
- execução das consultas SQL;
- visualização das tabelas no Unity Catalog;
- volumetria das camadas.

O índice completo está em [`docs/evidencias/README.md`](docs/evidencias/README.md).

---

## Decisões técnicas

As principais decisões foram registradas em formato de ADR simplificado em:

[`docs/decisions.md`](docs/decisions.md)

Entre os pontos documentados estão:

- ingestão por hash em vez de Auto Loader neste ambiente;
- reconstrução da timeline do SCD2 para tratar múltiplas versões e dados fora de ordem;
- uso de sentinela na primeira vigência do SCD2;
- tratamento de estorno como total pela ausência de valor parcial na origem;
- preservação das transações na fato;
- recomputação de agregados mensais afetados;
- uso do `batch_id` como principal escopo incremental da Ouro;
- ajustes necessários para execução local no Windows;
- compatibilidade com Databricks Serverless;
- compatibilidade com Python 3.10 no ambiente Databricks.

---

## Contratos de dados

Grão, chaves, colunas e regras dos principais objetos estão documentados em:

[`docs/data_contracts.md`](docs/data_contracts.md)

Esse documento serve como referência para quem precisar consumir a camada Ouro ou entender os objetos da Prata.

---

## Trade-offs e limitações

Algumas decisões foram mantidas simples de propósito para que o projeto continue reproduzível e compatível com o ambiente utilizado no desafio.

### Auto Loader

O Auto Loader não foi implementado porque a solução não depende de um object storage cloud externo para ser reproduzida.

Em produção, a ingestão poderia migrar para:

```python
spark.readStream.format("cloudFiles")
```

com checkpoint e evolução de schema gerenciados pelo Databricks.

### Estorno parcial

A origem possui o registro do estorno, mas não possui o valor estornado. Por isso, um estorno vinculado à transação é tratado como estorno total.

Se o valor parcial estivesse disponível, a regra poderia ser alterada para:

```text
valor_liquido = valor_transacao - soma(valor_estornado)
```

### CDC com delete físico

A massa utilizada possui operações de inserção e atualização. O fechamento por operação `D` não foi implementado neste escopo.

### Volume

A massa é sintética e pequena. O projeto não pretende representar um benchmark de performance. As decisões para escalabilidade estão descritas na documentação de arquitetura.

### Permissões de Unity Catalog

O registro dos objetos foi validado, mas a segregação completa de permissões por grupo depende de um workspace corporativo e privilégios administrativos.

---

## Evolução para produção

A evolução natural da solução incluiria:

- Auto Loader sobre ADLS/S3/GCS;
- Databricks Workflows com tasks Bronze -> Prata -> Ouro;
- alertas de falha e SLA;
- monitoramento da taxa de quarentena;
- métricas de volumetria por execução;
- permissões de Unity Catalog por grupos;
- Liquid Clustering/OPTIMIZE conforme crescimento da fato;
- testes de carga e performance;
- tratamento explícito de CDC delete;
- suporte a estorno parcial caso o campo passe a existir na origem;
- integração com ferramenta de observabilidade/lineage em ambiente produtivo.

---

## Documentação complementar

| Documento | Conteúdo |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Arquitetura, idempotência, performance, operação e governança |
| [`docs/decisions.md`](docs/decisions.md) | Decisões técnicas e trade-offs |
| [`docs/data_contracts.md`](docs/data_contracts.md) | Grão, chaves e regras das tabelas |
| [`docs/evidencias/README.md`](docs/evidencias/README.md) | Índice das evidências de execução |
| [`data/raw/nova_rota_input/README_DADOS.md`](data/raw/nova_rota_input/README_DADOS.md) | Descrição da massa de dados sintética |

---

## Resumo da solução

O projeto busca demonstrar uma implementação de Lakehouse que seja simples de executar, modular e rastreável.

O fluxo principal pode ser resumido em:

```text
Arquivos de origem
       |
       v
Bronze
- ingestão
- lineage
- idempotência
       |
       v
Prata
- limpeza
- qualidade
- SCD2
- MERGE
- quarentena
       |
       v
Ouro
- fato
- dimensões
- métricas
- risco
- features
       |
       v
BI / Analytics / Data Science
```

A intenção foi manter as decisões técnicas explícitas e separar o que foi de fato implementado do que seria uma evolução natural em um ambiente produtivo de maior escala.
