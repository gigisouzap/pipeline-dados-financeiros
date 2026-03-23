-- ================================================================
-- 03 - CONSULTAS BÁSICAS (Exploração Inicial dos Dados)
-- Pipeline de Dados Financeiros
--
-- CONCEITO DE ENGENHARIA DE DADOS:
-- Antes de construir qualquer pipeline, o engenheiro de dados
-- faz uma exploração inicial: entende o volume, a qualidade
-- e o formato dos dados. Esse passo é chamado de
-- "Data Profiling" ou "EDA — Exploratory Data Analysis".
-- ================================================================

USE banco_digital;


-- -----------------------------------------------
-- 1. Quantos registros temos em cada tabela?
--    (Checagem básica após ingestão)
-- -----------------------------------------------
SELECT 'clientes'    AS tabela, COUNT(*) AS total FROM clientes    UNION ALL
SELECT 'contas',                COUNT(*)           FROM contas      UNION ALL
SELECT 'cartoes',               COUNT(*)           FROM cartoes     UNION ALL
SELECT 'transacoes',            COUNT(*)           FROM transacoes;
-- UNION ALL empilha os resultados de vários SELECTs


-- -----------------------------------------------
-- 2. Visualizar todos os clientes ativos
-- -----------------------------------------------
SELECT
    id_cliente,
    nome,
    cidade,
    estado,
    renda_mensal
FROM clientes
WHERE ativo = TRUE
ORDER BY renda_mensal DESC;


-- -----------------------------------------------
-- 3. Contas com saldo negativo ou muito baixo
--    (alerta de risco financeiro)
-- -----------------------------------------------
SELECT
    c.numero_conta,
    cl.nome        AS cliente,
    c.tipo_conta,
    c.saldo
FROM contas c
INNER JOIN clientes cl ON c.id_cliente = cl.id_cliente
WHERE c.saldo < 500.00
ORDER BY c.saldo ASC;


-- -----------------------------------------------
-- 4. Todas as transações suspeitas
--    (primeiro olhar sobre anomalias)
-- -----------------------------------------------
SELECT
    id_transacao,
    id_conta,
    tipo,
    valor,
    data_hora,
    descricao,
    cidade_origem
FROM transacoes
WHERE suspeita_fraude = TRUE
ORDER BY data_hora;


-- -----------------------------------------------
-- 5. Transações de alto valor (acima de R$ 3.000)
-- -----------------------------------------------
SELECT
    id_transacao,
    id_conta,
    tipo,
    valor,
    data_hora,
    cidade_origem
FROM transacoes
WHERE valor > 3000.00
ORDER BY valor DESC;


-- -----------------------------------------------
-- 6. Transações de madrugada (entre 0h e 5h)
--    HOUR() extrai a hora de um DATETIME
-- -----------------------------------------------
SELECT
    id_transacao,
    id_conta,
    tipo,
    valor,
    data_hora,
    suspeita_fraude
FROM transacoes
WHERE HOUR(data_hora) BETWEEN 0 AND 5
ORDER BY data_hora;
