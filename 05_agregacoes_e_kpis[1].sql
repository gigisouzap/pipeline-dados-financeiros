-- ================================================================
-- 05 - AGREGAÇÕES E KPIs FINANCEIROS
-- Pipeline de Dados Financeiros
--
-- CONCEITO DE ENGENHARIA DE DADOS:
-- A camada "Gold" da arquitetura Medallion é onde os dados
-- viram informação de negócio: KPIs, dashboards, relatórios.
-- O engenheiro de dados constrói as queries que alimentam
-- essas visões analíticas — exatamente o que praticamos aqui.
-- ================================================================

USE banco_digital;


-- -----------------------------------------------
-- 1. Volume total movimentado por tipo de transação
--    KPI de operações
-- -----------------------------------------------
SELECT
    tipo,
    COUNT(*)            AS qtd_transacoes,
    SUM(valor)          AS volume_total,
    ROUND(AVG(valor),2) AS ticket_medio,
    MAX(valor)          AS maior_transacao
FROM transacoes
GROUP BY tipo
ORDER BY volume_total DESC;


-- -----------------------------------------------
-- 2. Gasto por categoria — onde o dinheiro vai?
--    (base para recomendações e análise de perfil)
-- -----------------------------------------------
SELECT
    ct.nome             AS categoria,
    ct.tipo,
    COUNT(t.id_transacao) AS qtd,
    SUM(t.valor)          AS total_movimentado
FROM transacoes t
INNER JOIN categorias_transacao ct ON t.id_categoria = ct.id_categoria
WHERE t.suspeita_fraude = FALSE  -- excluímos transações suspeitas da análise
GROUP BY ct.id_categoria, ct.nome, ct.tipo
ORDER BY total_movimentado DESC;


-- -----------------------------------------------
-- 3. Movimentação mensal — evolução no tempo
--    Fundamental para análise de tendências
-- -----------------------------------------------
SELECT
    YEAR(data_hora)   AS ano,
    MONTH(data_hora)  AS mes,
    COUNT(*)          AS qtd_transacoes,
    SUM(valor)        AS volume_total,
    SUM(CASE WHEN tipo = 'credito' THEN valor ELSE 0 END) AS total_entradas,
    SUM(CASE WHEN tipo != 'credito' THEN valor ELSE 0 END) AS total_saidas
    -- CASE WHEN separa entradas de saídas em colunas distintas
FROM transacoes
GROUP BY ano, mes
ORDER BY ano, mes;


-- -----------------------------------------------
-- 4. Ranking de clientes por volume movimentado
--    (segmentação: quem são os clientes VIP?)
-- -----------------------------------------------
SELECT
    cl.nome               AS cliente,
    cl.renda_mensal,
    COUNT(t.id_transacao) AS qtd_transacoes,
    SUM(t.valor)          AS volume_total,
    ROUND(AVG(t.valor),2) AS ticket_medio
FROM transacoes t
INNER JOIN contas    c  ON t.id_conta   = c.id_conta
INNER JOIN clientes  cl ON c.id_cliente = cl.id_cliente
WHERE t.suspeita_fraude = FALSE
GROUP BY cl.id_cliente, cl.nome, cl.renda_mensal
ORDER BY volume_total DESC;


-- -----------------------------------------------
-- 5. Taxa de suspeita de fraude por conta
--    HAVING filtra contas com pelo menos 1 ocorrência
-- -----------------------------------------------
SELECT
    c.numero_conta,
    cl.nome                AS cliente,
    COUNT(*)               AS total_transacoes,
    SUM(t.suspeita_fraude) AS qtd_suspeitas,
    ROUND(
        SUM(t.suspeita_fraude) * 100.0 / COUNT(*), 1
    )                      AS pct_suspeitas
FROM transacoes t
INNER JOIN contas    c  ON t.id_conta   = c.id_conta
INNER JOIN clientes  cl ON c.id_cliente = cl.id_cliente
GROUP BY c.id_conta, c.numero_conta, cl.nome
HAVING SUM(t.suspeita_fraude) > 0
ORDER BY pct_suspeitas DESC;
