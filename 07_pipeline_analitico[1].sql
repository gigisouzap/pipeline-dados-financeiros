-- ================================================================
-- 07 - PIPELINE ANALÍTICO (Camada Gold)
-- Pipeline de Dados Financeiros
--
-- CONCEITO DE ENGENHARIA DE DADOS:
-- A camada Gold é o destino final do pipeline ETL/ELT.
-- São as views, tabelas agregadas e relatórios que o time
-- de negócio, Data Scientists e analistas consomem.
-- Em produção, essas queries viram VIEWs no banco ou
-- tabelas materializadas no Data Warehouse (BigQuery, Redshift).
-- ================================================================

USE banco_digital;


-- ================================================================
-- VIEW: vw_extrato_completo
-- Substitui vários JOINs repetitivos — boa prática de pipeline
-- ================================================================
CREATE OR REPLACE VIEW vw_extrato_completo AS
SELECT
    t.id_transacao,
    cl.id_cliente,
    cl.nome              AS cliente,
    cl.cidade            AS cidade_cliente,
    c.numero_conta,
    c.tipo_conta,
    ct.nome              AS categoria,
    ct.tipo              AS tipo_categoria,
    t.tipo               AS tipo_transacao,
    t.valor,
    t.data_hora,
    YEAR(t.data_hora)    AS ano,
    MONTH(t.data_hora)   AS mes,
    HOUR(t.data_hora)    AS hora,
    t.descricao,
    t.cidade_origem,
    t.suspeita_fraude
FROM transacoes t
INNER JOIN contas                c  ON t.id_conta     = c.id_conta
INNER JOIN clientes              cl ON c.id_cliente   = cl.id_cliente
INNER JOIN categorias_transacao  ct ON t.id_categoria = ct.id_categoria;

-- A partir daqui, as queries ficam muito mais simples:
-- SELECT * FROM vw_extrato_completo WHERE cliente = 'Ana Paula Rodrigues';


-- ================================================================
-- RELATÓRIO 1: Dashboard executivo — resumo do banco
-- ================================================================
SELECT
    COUNT(DISTINCT id_cliente)    AS total_clientes,
    COUNT(DISTINCT numero_conta)  AS total_contas,
    COUNT(id_transacao)           AS total_transacoes,
    ROUND(SUM(valor), 2)          AS volume_total_movimentado,
    ROUND(AVG(valor), 2)          AS ticket_medio_global,
    SUM(suspeita_fraude)          AS alertas_fraude
FROM vw_extrato_completo;


-- ================================================================
-- RELATÓRIO 2: Perfil financeiro por cliente
-- (alimenta um dashboard de CRM ou recomendação)
-- ================================================================
SELECT
    cliente,
    COUNT(id_transacao)           AS qtd_transacoes,
    SUM(CASE WHEN tipo_categoria = 'receita' THEN valor ELSE 0 END) AS total_entradas,
    SUM(CASE WHEN tipo_categoria = 'despesa' THEN valor ELSE 0 END) AS total_saidas,
    ROUND(
        SUM(CASE WHEN tipo_categoria = 'receita' THEN valor ELSE 0 END) -
        SUM(CASE WHEN tipo_categoria = 'despesa' THEN valor ELSE 0 END),
    2)                            AS saldo_periodo,
    SUM(suspeita_fraude)          AS alertas_fraude,
    -- Classificação de perfil baseada em volume
    CASE
        WHEN SUM(valor) > 20000 THEN 'VIP'
        WHEN SUM(valor) > 5000  THEN 'Intermediário'
        ELSE                         'Básico'
    END                           AS perfil_cliente
FROM vw_extrato_completo
WHERE suspeita_fraude = FALSE
GROUP BY id_cliente, cliente
ORDER BY total_entradas DESC;


-- ================================================================
-- RELATÓRIO 3: Heatmap de risco — hora vs. dia da semana
-- Identifica janelas de tempo com mais fraudes
-- ================================================================
SELECT
    DAYOFWEEK(data_hora)  AS dia_semana,  -- 1=Domingo, 7=Sábado
    HOUR(data_hora)       AS hora,
    COUNT(*)              AS total_transacoes,
    SUM(suspeita_fraude)  AS qtd_suspeitas,
    ROUND(
        SUM(suspeita_fraude) * 100.0 / COUNT(*), 1
    )                     AS pct_suspeitas
FROM vw_extrato_completo
GROUP BY dia_semana, hora
HAVING SUM(suspeita_fraude) > 0
ORDER BY pct_suspeitas DESC;


-- ================================================================
-- RELATÓRIO 4: Top categorias de gasto por perfil de renda
-- Segmenta comportamento financeiro por faixa de renda
-- ================================================================
SELECT
    CASE
        WHEN cl.renda_mensal < 3000  THEN 'Baixa renda (até 3k)'
        WHEN cl.renda_mensal < 8000  THEN 'Média renda (3k-8k)'
        ELSE                              'Alta renda (8k+)'
    END                  AS faixa_renda,
    v.categoria,
    COUNT(*)             AS qtd_transacoes,
    ROUND(SUM(v.valor),2) AS gasto_total
FROM vw_extrato_completo v
INNER JOIN clientes cl ON v.id_cliente = cl.id_cliente
WHERE v.tipo_categoria = 'despesa'
  AND v.suspeita_fraude = FALSE
GROUP BY faixa_renda, v.categoria
ORDER BY faixa_renda, gasto_total DESC;
