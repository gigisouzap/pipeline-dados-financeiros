-- ================================================================
-- 06 - DETECÇÃO DE ANOMALIAS E FRAUDES
-- Pipeline de Dados Financeiros
--
-- CONCEITO DE ENGENHARIA DE DADOS:
-- Detecção de fraudes é um dos casos de uso mais críticos
-- em dados financeiros. Engenheiros de dados constroem
-- pipelines que aplicam regras de negócio (rules-based)
-- para sinalizar transações suspeitas em tempo real ou batch.
-- Aqui praticamos esse raciocínio com SQL puro.
-- ================================================================

USE banco_digital;


-- -----------------------------------------------
-- 1. Padrão "Smurfing": vários saques pequenos
--    em sequência no mesmo dia
--    (fragmentar valores para não acionar limites)
-- -----------------------------------------------
SELECT
    t.id_conta,
    cl.nome              AS cliente,
    DATE(t.data_hora)    AS data,
    COUNT(*)             AS qtd_saques,
    SUM(t.valor)         AS total_sacado
FROM transacoes t
INNER JOIN contas    c  ON t.id_conta   = c.id_conta
INNER JOIN clientes  cl ON c.id_cliente = cl.id_cliente
WHERE t.tipo = 'saque'
GROUP BY t.id_conta, cl.nome, DATE(t.data_hora)
HAVING COUNT(*) >= 3          -- 3 ou mais saques no mesmo dia
ORDER BY qtd_saques DESC;


-- -----------------------------------------------
-- 2. Transações fora da cidade de cadastro
--    Compara cidade_origem com cidade do cliente
-- -----------------------------------------------
SELECT
    cl.nome              AS cliente,
    cl.cidade            AS cidade_cadastro,
    t.cidade_origem,
    t.tipo,
    t.valor,
    t.data_hora,
    t.descricao
FROM transacoes t
INNER JOIN contas    c  ON t.id_conta   = c.id_conta
INNER JOIN clientes  cl ON c.id_cliente = cl.id_cliente
WHERE t.cidade_origem != cl.cidade    -- cidades diferentes = alerta
  AND t.cidade_origem IS NOT NULL
ORDER BY t.valor DESC;


-- -----------------------------------------------
-- 3. Transações de alto valor fora do horário comercial
--    (combinação de fatores = risco elevado)
-- -----------------------------------------------
SELECT
    cl.nome              AS cliente,
    t.tipo,
    t.valor,
    t.data_hora,
    t.cidade_origem,
    HOUR(t.data_hora)    AS hora
FROM transacoes t
INNER JOIN contas    c  ON t.id_conta   = c.id_conta
INNER JOIN clientes  cl ON c.id_cliente = cl.id_cliente
WHERE t.valor > 3000
  AND HOUR(t.data_hora) NOT BETWEEN 8 AND 18  -- fora do horário comercial
ORDER BY t.valor DESC;


-- -----------------------------------------------
-- 4. Transações acima de 3x a média do cliente
--    Subquery correlacionada — padrão avançado
-- -----------------------------------------------
SELECT
    cl.nome              AS cliente,
    t.valor              AS valor_transacao,
    t.data_hora,
    t.descricao,
    (
        -- Subquery: calcula a média desse cliente específico
        SELECT ROUND(AVG(t2.valor), 2)
        FROM transacoes t2
        WHERE t2.id_conta = t.id_conta
    )                    AS media_cliente
FROM transacoes t
INNER JOIN contas    c  ON t.id_conta   = c.id_conta
INNER JOIN clientes  cl ON c.id_cliente = cl.id_cliente
WHERE t.valor > 3 * (
    SELECT AVG(t3.valor)
    FROM transacoes t3
    WHERE t3.id_conta = t.id_conta  -- média individual por conta
)
ORDER BY t.valor DESC;


-- -----------------------------------------------
-- 5. Score de risco por cliente
--    Combina múltiplos sinais em uma nota
-- -----------------------------------------------
SELECT
    cl.nome                       AS cliente,
    COUNT(t.id_transacao)         AS total_transacoes,
    SUM(t.suspeita_fraude)        AS qtd_suspeitas,
    SUM(CASE WHEN HOUR(t.data_hora) NOT BETWEEN 8 AND 18 THEN 1 ELSE 0 END)
                                  AS transacoes_noturnas,
    SUM(CASE WHEN t.cidade_origem != cl.cidade THEN 1 ELSE 0 END)
                                  AS transacoes_fora_cidade,
    -- Score simples: soma ponderada dos sinais de risco
    (
        SUM(t.suspeita_fraude) * 3 +
        SUM(CASE WHEN HOUR(t.data_hora) NOT BETWEEN 8 AND 18 THEN 1 ELSE 0 END) * 2 +
        SUM(CASE WHEN t.cidade_origem != cl.cidade THEN 1 ELSE 0 END) * 1
    )                             AS score_risco
FROM transacoes t
INNER JOIN contas    c  ON t.id_conta   = c.id_conta
INNER JOIN clientes  cl ON c.id_cliente = cl.id_cliente
GROUP BY cl.id_cliente, cl.nome
ORDER BY score_risco DESC;
