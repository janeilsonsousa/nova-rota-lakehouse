# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Análises SQL avançadas
# MAGIC
# MAGIC Executa as consultas de `sql/advanced_queries.sql` (CTEs encadeadas,
# MAGIC `ROW_NUMBER`, `LAG`/`LEAD`, `FIRST_VALUE`/`LAST_VALUE`,
# MAGIC `NTILE`/`PERCENT_RANK`, detecção de anomalia, comparação cliente vs.
# MAGIC histórico/cidade/segmento, `MERGE INTO`, `EXPLAIN FORMATTED`).
# MAGIC
# MAGIC **Nota de portabilidade**: `sql/advanced_queries.sql` (o artefato de
# MAGIC entrega) usa nomes plenamente qualificados de catálogo
# MAGIC (`nova_rota.gold.gold_fato_transacao`) — o padrão correto quando as
# MAGIC tabelas são *managed tables* do Unity Catalog em produção. Neste
# MAGIC ambiente de demonstração as tabelas são Delta **path-based** (gravadas
# MAGIC em um Volume — ver notebook `00`), e Volumes não são aceitos como
# MAGIC `LOCATION` de tabela registrada no catálogo (exigiria um External
# MAGIC Location com storage credential, fora do escopo de uma conta pessoal
# MAGIC gratuita). Por isso este notebook registra **temp views** apontando
# MAGIC para os mesmos paths físicos e roda a *mesma lógica* das queries
# MAGIC contra essas views — o SQL é idêntico, só a forma de apontar para a
# MAGIC tabela muda.
# MAGIC
# MAGIC **Pré-requisito**: rodar `00`, `01`, `02` e `03` antes deste notebook.

# COMMAND ----------

import sys
from pathlib import Path

repo_root = Path.cwd()
while not (repo_root / "src").exists() and repo_root != repo_root.parent:
    repo_root = repo_root.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

# COMMAND ----------

dbutils.widgets.text("catalog", "nova_rota")
dbutils.widgets.text("base_path", "/Volumes/nova_rota/bronze/storage/lakehouse")

# COMMAND ----------

from src.config import get_config  # noqa: E402

config = get_config(env="databricks", catalog=dbutils.widgets.get("catalog"), base_path=dbutils.widgets.get("base_path"))

tabelas = {
    "gold_fato_transacao": ("gold", "gold_fato_transacao"),
    "gold_dim_cliente": ("gold", "gold_dim_cliente"),
    "gold_cliente_mes": ("gold", "gold_cliente_mes"),
    "silver_transacoes": ("silver", "silver_transacoes"),
    "silver_clientes": ("silver", "silver_clientes"),
    "silver_contas": ("silver", "silver_contas"),
    "silver_cartoes": ("silver", "silver_cartoes"),
    "bronze_transacoes": ("bronze", "bronze_transacoes"),
}
for view_name, (layer, table) in tabelas.items():
    df = spark.read.format("delta").load(config.table_path(layer, table))
    df.createOrReplaceTempView(view_name)
    print(f"view registrada: {view_name} ({df.count()} linhas)")

# COMMAND ----------

# MAGIC %md ## 1) CTEs encadeadas + ROW_NUMBER — transação de maior valor por cliente/mês

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH transacoes_com_mes AS (
# MAGIC     SELECT
# MAGIC         f.id_transacao, f.id_cliente_na_data, f.dt_transacao,
# MAGIC         DATE_FORMAT(f.dt_transacao, 'yyyy-MM') AS ano_mes, f.valor_liquido
# MAGIC     FROM gold_fato_transacao f
# MAGIC     WHERE f.flag_estornada = FALSE
# MAGIC ),
# MAGIC ranking_mensal AS (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY id_cliente_na_data, ano_mes ORDER BY valor_liquido DESC) AS rank_valor_no_mes
# MAGIC     FROM transacoes_com_mes
# MAGIC ),
# MAGIC top_transacao_por_cliente_mes AS (SELECT * FROM ranking_mensal WHERE rank_valor_no_mes = 1)
# MAGIC SELECT t.id_cliente_na_data AS id_cliente, t.ano_mes, t.id_transacao AS id_transacao_maior_valor,
# MAGIC        t.valor_liquido AS maior_valor_liquido_do_mes, d.segmento, d.cidade
# MAGIC FROM top_transacao_por_cliente_mes t
# MAGIC JOIN gold_dim_cliente d ON d.id_cliente = t.id_cliente_na_data
# MAGIC ORDER BY t.ano_mes, maior_valor_liquido_do_mes DESC;

# COMMAND ----------

# MAGIC %md ## 2) LAG / LEAD — comportamento entre meses consecutivos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     id_cliente, ano_mes, valor_liquido,
# MAGIC     LAG(valor_liquido) OVER (PARTITION BY id_cliente ORDER BY ano_mes)  AS valor_liquido_mes_anterior,
# MAGIC     LEAD(valor_liquido) OVER (PARTITION BY id_cliente ORDER BY ano_mes) AS valor_liquido_mes_seguinte,
# MAGIC     ROUND((valor_liquido - LAG(valor_liquido) OVER (PARTITION BY id_cliente ORDER BY ano_mes))
# MAGIC           / NULLIF(LAG(valor_liquido) OVER (PARTITION BY id_cliente ORDER BY ano_mes), 0) * 100, 2) AS variacao_valor_pct
# MAGIC FROM gold_cliente_mes
# MAGIC ORDER BY id_cliente, ano_mes;

# COMMAND ----------

# MAGIC %md ## 3) FIRST_VALUE / LAST_VALUE — primeira e última transação por cliente

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC     id_cliente_na_data AS id_cliente,
# MAGIC     FIRST_VALUE(id_transacao) OVER (PARTITION BY id_cliente_na_data ORDER BY dt_transacao, id_transacao ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS primeira_transacao_id,
# MAGIC     FIRST_VALUE(valor) OVER (PARTITION BY id_cliente_na_data ORDER BY dt_transacao, id_transacao ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS primeira_transacao_valor,
# MAGIC     LAST_VALUE(id_transacao) OVER (PARTITION BY id_cliente_na_data ORDER BY dt_transacao, id_transacao ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ultima_transacao_id,
# MAGIC     LAST_VALUE(valor) OVER (PARTITION BY id_cliente_na_data ORDER BY dt_transacao, id_transacao ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ultima_transacao_valor
# MAGIC FROM gold_fato_transacao
# MAGIC ORDER BY id_cliente;

# COMMAND ----------

# MAGIC %md ## 4) NTILE / PERCENT_RANK — segmentação de clientes por gasto

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH gasto_cliente AS (
# MAGIC     SELECT id_cliente, SUM(valor_liquido) AS valor_liquido_total FROM gold_cliente_mes GROUP BY id_cliente
# MAGIC )
# MAGIC SELECT id_cliente, valor_liquido_total,
# MAGIC        NTILE(10) OVER (ORDER BY valor_liquido_total) AS decil_gasto,
# MAGIC        ROUND(PERCENT_RANK() OVER (ORDER BY valor_liquido_total), 4) AS percentil_gasto
# MAGIC FROM gasto_cliente
# MAGIC ORDER BY valor_liquido_total DESC;

# COMMAND ----------

# MAGIC %md ## 5) Detecção de anomalias — transações com valor muito acima do padrão do próprio cliente

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH stats_cliente AS (
# MAGIC     SELECT id_cliente_na_data AS id_cliente, AVG(valor) media_valor, STDDEV_POP(valor) desvio_valor, COUNT(*) qtd
# MAGIC     FROM gold_fato_transacao GROUP BY id_cliente_na_data HAVING COUNT(*) >= 3
# MAGIC )
# MAGIC SELECT f.id_transacao, f.id_cliente_na_data AS id_cliente, f.dt_transacao, f.valor, s.media_valor, s.desvio_valor,
# MAGIC        ROUND((f.valor - s.media_valor) / NULLIF(s.desvio_valor, 0), 2) AS z_score
# MAGIC FROM gold_fato_transacao f
# MAGIC JOIN stats_cliente s ON s.id_cliente = f.id_cliente_na_data
# MAGIC WHERE s.desvio_valor > 0 AND (f.valor - s.media_valor) / s.desvio_valor >= 1.5
# MAGIC ORDER BY z_score DESC;

# COMMAND ----------

# MAGIC %md ## 6) Cliente vs. próprio histórico E vs. cidade/segmento

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cliente_mes_enriquecido AS (
# MAGIC     SELECT cm.id_cliente, cm.ano_mes, cm.ticket_medio, d.cidade, d.segmento
# MAGIC     FROM gold_cliente_mes cm JOIN gold_dim_cliente d ON d.id_cliente = cm.id_cliente
# MAGIC ),
# MAGIC media_historica_cliente AS (
# MAGIC     SELECT id_cliente, AVG(ticket_medio) AS ticket_medio_historico_proprio FROM cliente_mes_enriquecido GROUP BY id_cliente
# MAGIC ),
# MAGIC media_cidade_segmento_mes AS (
# MAGIC     SELECT cidade, segmento, ano_mes, AVG(ticket_medio) AS ticket_medio_cidade_segmento
# MAGIC     FROM cliente_mes_enriquecido GROUP BY cidade, segmento, ano_mes
# MAGIC )
# MAGIC SELECT c.id_cliente, c.ano_mes, c.cidade, c.segmento, c.ticket_medio AS ticket_medio_do_mes,
# MAGIC        h.ticket_medio_historico_proprio,
# MAGIC        ROUND(c.ticket_medio - h.ticket_medio_historico_proprio, 2) AS diff_vs_proprio_historico,
# MAGIC        cs.ticket_medio_cidade_segmento,
# MAGIC        ROUND(c.ticket_medio - cs.ticket_medio_cidade_segmento, 2) AS diff_vs_cidade_segmento
# MAGIC FROM cliente_mes_enriquecido c
# MAGIC JOIN media_historica_cliente h ON h.id_cliente = c.id_cliente
# MAGIC JOIN media_cidade_segmento_mes cs
# MAGIC     ON cs.cidade = c.cidade AND cs.segmento = c.segmento AND cs.ano_mes = c.ano_mes
# MAGIC ORDER BY c.id_cliente, c.ano_mes;

# COMMAND ----------

# MAGIC %md ## 7) MERGE INTO — carga incremental idempotente em SQL
# MAGIC
# MAGIC Alvo e fonte referenciados por **path Delta direto**
# MAGIC (`` delta.`/Volumes/...` ``) em vez de nome de catálogo — mesma razão
# MAGIC de portabilidade explicada no topo do notebook; a sintaxe
# MAGIC `MERGE INTO ... WHEN MATCHED ... WHEN NOT MATCHED` é idêntica à de
# MAGIC produção.

# COMMAND ----------

silver_transacoes_path = config.table_path("silver", "silver_transacoes")
bronze_transacoes_path = config.table_path("bronze", "bronze_transacoes")
algum_batch_id = spark.read.format("delta").load(bronze_transacoes_path).select("batch_id").first()["batch_id"]
print("silver_transacoes_path:", silver_transacoes_path)
print("bronze_transacoes_path:", bronze_transacoes_path)
print("batch_id de exemplo:", algum_batch_id)

# COMMAND ----------

spark.sql(f"""
MERGE INTO delta.`{silver_transacoes_path}` AS t
USING (
    SELECT
        id_transacao, id_cartao, data_transacao, CAST(data_transacao AS DATE) AS dt_transacao,
        valor, mcc, estabelecimento, canal, pais, moeda, device_id, ip_origem,
        schema_version, arquivo_origem, batch_id, timestamp_ingestao,
        CURRENT_TIMESTAMP() AS timestamp_processamento_silver
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY id_transacao ORDER BY timestamp_ingestao DESC
        ) AS rn
        FROM delta.`{bronze_transacoes_path}`
        WHERE batch_id = '{algum_batch_id}'
    )
    WHERE rn = 1  -- mesma id_transacao pode se repetir dentro do lote (ver src/silver/transacoes.py)
) AS s
ON t.id_transacao = s.id_transacao
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""").show()

# COMMAND ----------

# MAGIC %md ## 8) Plano de execução — estratégia de join ponto-no-tempo

# COMMAND ----------

spark.sql("""
EXPLAIN FORMATTED
SELECT f.id_transacao, c.cidade
FROM gold_fato_transacao f
JOIN silver_clientes c
    ON c.id_cliente = f.id_cliente_na_data
    AND f.data_transacao >= c.dt_inicio_vigencia
    AND (c.dt_fim_vigencia IS NULL OR f.data_transacao < c.dt_fim_vigencia)
""").show(truncate=False)
