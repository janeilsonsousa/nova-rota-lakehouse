-- Queries usam nova_rota.gold.<tabela>. Local sem Unity Catalog, tira o prefixo nova_rota.

-- 1) CTEs + ROW_NUMBER: maior transação do cliente por mês
WITH transacoes_com_mes AS (
    SELECT
        f.id_transacao,
        f.id_cliente_na_data,
        f.dt_transacao,
        DATE_FORMAT(f.dt_transacao, 'yyyy-MM') AS ano_mes,
        f.valor_liquido
    FROM nova_rota.gold.gold_fato_transacao f
    WHERE f.flag_estornada = FALSE
),
ranking_mensal AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_cliente_na_data, ano_mes
            ORDER BY valor_liquido DESC
        ) AS rank_valor_no_mes
    FROM transacoes_com_mes
),
top_transacao_por_cliente_mes AS (
    SELECT *
    FROM ranking_mensal
    WHERE rank_valor_no_mes = 1
)
SELECT
    t.id_cliente_na_data AS id_cliente,
    t.ano_mes,
    t.id_transacao AS id_transacao_maior_valor,
    t.valor_liquido AS maior_valor_liquido_do_mes,
    d.segmento,
    d.cidade
FROM top_transacao_por_cliente_mes t
JOIN nova_rota.gold.gold_dim_cliente d ON d.id_cliente = t.id_cliente_na_data
ORDER BY t.ano_mes, maior_valor_liquido_do_mes DESC;


-- 2) LAG / LEAD: comportamento entre meses consecutivos
SELECT
    id_cliente,
    ano_mes,
    valor_liquido,
    LAG(valor_liquido) OVER (PARTITION BY id_cliente ORDER BY ano_mes)  AS valor_liquido_mes_anterior,
    LEAD(valor_liquido) OVER (PARTITION BY id_cliente ORDER BY ano_mes) AS valor_liquido_mes_seguinte,
    qtd_transacoes,
    LAG(qtd_transacoes) OVER (PARTITION BY id_cliente ORDER BY ano_mes) AS qtd_transacoes_mes_anterior,
    ROUND(
        (valor_liquido - LAG(valor_liquido) OVER (PARTITION BY id_cliente ORDER BY ano_mes))
        / NULLIF(LAG(valor_liquido) OVER (PARTITION BY id_cliente ORDER BY ano_mes), 0) * 100,
        2
    ) AS variacao_valor_pct
FROM nova_rota.gold.gold_cliente_mes
ORDER BY id_cliente, ano_mes;


-- 3) FIRST_VALUE / LAST_VALUE: primeira e última transação de cada cliente
SELECT DISTINCT
    id_cliente_na_data AS id_cliente,
    FIRST_VALUE(id_transacao) OVER (
        PARTITION BY id_cliente_na_data ORDER BY dt_transacao, id_transacao
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS primeira_transacao_id,
    FIRST_VALUE(dt_transacao) OVER (
        PARTITION BY id_cliente_na_data ORDER BY dt_transacao, id_transacao
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS primeira_transacao_data,
    FIRST_VALUE(valor) OVER (
        PARTITION BY id_cliente_na_data ORDER BY dt_transacao, id_transacao
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS primeira_transacao_valor,
    LAST_VALUE(id_transacao) OVER (
        PARTITION BY id_cliente_na_data ORDER BY dt_transacao, id_transacao
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS ultima_transacao_id,
    LAST_VALUE(dt_transacao) OVER (
        PARTITION BY id_cliente_na_data ORDER BY dt_transacao, id_transacao
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS ultima_transacao_data,
    LAST_VALUE(valor) OVER (
        PARTITION BY id_cliente_na_data ORDER BY dt_transacao, id_transacao
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS ultima_transacao_valor
FROM nova_rota.gold.gold_fato_transacao
ORDER BY id_cliente;


-- 4) NTILE / PERCENT_RANK: segmentação de clientes por gasto
WITH gasto_cliente AS (
    SELECT
        id_cliente,
        SUM(valor_liquido) AS valor_liquido_total
    FROM nova_rota.gold.gold_cliente_mes
    GROUP BY id_cliente
)
SELECT
    id_cliente,
    valor_liquido_total,
    NTILE(10) OVER (ORDER BY valor_liquido_total)        AS decil_gasto,
    ROUND(PERCENT_RANK() OVER (ORDER BY valor_liquido_total), 4) AS percentil_gasto,
    CASE
        WHEN NTILE(10) OVER (ORDER BY valor_liquido_total) >= 9 THEN 'TOP_20_PCT'
        WHEN NTILE(10) OVER (ORDER BY valor_liquido_total) <= 2 THEN 'BOTTOM_20_PCT'
        ELSE 'MEIO'
    END AS faixa_gasto
FROM gasto_cliente
ORDER BY valor_liquido_total DESC;


-- 5) anomalia: transação com valor muito acima do padrão do próprio cliente (z-score)
WITH stats_cliente AS (
    SELECT
        id_cliente_na_data AS id_cliente,
        AVG(valor)          AS media_valor,
        STDDEV_POP(valor)   AS desvio_valor,
        COUNT(*)            AS qtd_transacoes_historico
    FROM nova_rota.gold.gold_fato_transacao
    GROUP BY id_cliente_na_data
    HAVING COUNT(*) >= 3
)
SELECT
    f.id_transacao,
    f.id_cliente_na_data AS id_cliente,
    f.dt_transacao,
    f.valor,
    s.media_valor,
    s.desvio_valor,
    ROUND((f.valor - s.media_valor) / NULLIF(s.desvio_valor, 0), 2) AS z_score,
    CASE
        WHEN s.desvio_valor = 0 THEN 'SEM_VARIACAO_HISTORICA'
        WHEN (f.valor - s.media_valor) / NULLIF(s.desvio_valor, 0) >= 3   THEN 'ANOMALIA_ALTA'
        WHEN (f.valor - s.media_valor) / NULLIF(s.desvio_valor, 0) >= 2   THEN 'ATENCAO'
        WHEN (f.valor - s.media_valor) / NULLIF(s.desvio_valor, 0) >= 1.5 THEN 'LEVE'
        ELSE 'NORMAL'
    END AS classificacao_anomalia
FROM nova_rota.gold.gold_fato_transacao f
JOIN stats_cliente s ON s.id_cliente = f.id_cliente_na_data
WHERE s.desvio_valor > 0
  AND (f.valor - s.media_valor) / s.desvio_valor >= 1.5
ORDER BY z_score DESC;


-- 6) cliente vs. próprio histórico e vs. cidade/segmento (ticket médio)
WITH cliente_mes_enriquecido AS (
    SELECT
        cm.id_cliente,
        cm.ano_mes,
        cm.ticket_medio,
        d.cidade,
        d.segmento
    FROM nova_rota.gold.gold_cliente_mes cm
    JOIN nova_rota.gold.gold_dim_cliente d ON d.id_cliente = cm.id_cliente
),
media_historica_cliente AS (
    SELECT
        id_cliente,
        AVG(ticket_medio) AS ticket_medio_historico_proprio
    FROM cliente_mes_enriquecido
    GROUP BY id_cliente
),
media_cidade_segmento_mes AS (
    SELECT
        cidade,
        segmento,
        ano_mes,
        AVG(ticket_medio) AS ticket_medio_cidade_segmento
    FROM cliente_mes_enriquecido
    GROUP BY cidade, segmento, ano_mes
)
SELECT
    c.id_cliente,
    c.ano_mes,
    c.cidade,
    c.segmento,
    c.ticket_medio                                 AS ticket_medio_do_mes,
    h.ticket_medio_historico_proprio,
    ROUND(c.ticket_medio - h.ticket_medio_historico_proprio, 2)      AS diff_vs_proprio_historico,
    cs.ticket_medio_cidade_segmento,
    ROUND(c.ticket_medio - cs.ticket_medio_cidade_segmento, 2)       AS diff_vs_cidade_segmento
FROM cliente_mes_enriquecido c
JOIN media_historica_cliente h ON h.id_cliente = c.id_cliente
JOIN media_cidade_segmento_mes cs
    ON cs.cidade = c.cidade AND cs.segmento = c.segmento AND cs.ano_mes = c.ano_mes
ORDER BY c.id_cliente, c.ano_mes;


-- 7) MERGE INTO em SQL puro (a implementação real fica em src/silver/transacoes.py)
MERGE INTO nova_rota.silver.silver_transacoes AS t
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
        FROM nova_rota.bronze.bronze_transacoes
        WHERE batch_id = :batch_id_atual
    )
    WHERE rn = 1  -- mesma id_transacao pode repetir dentro do lote
) AS s
ON t.id_transacao = s.id_transacao
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;


-- 8) plano de execução do join ponto-no-tempo fato x dimensão SCD2 (espera BroadcastHashJoin)
EXPLAIN FORMATTED
SELECT f.id_transacao, c.cidade
FROM nova_rota.gold.gold_fato_transacao f
JOIN nova_rota.silver.silver_clientes c
    ON c.id_cliente = f.id_cliente_na_data
    AND f.data_transacao >= c.dt_inicio_vigencia
    AND (c.dt_fim_vigencia IS NULL OR f.data_transacao < c.dt_fim_vigencia);
