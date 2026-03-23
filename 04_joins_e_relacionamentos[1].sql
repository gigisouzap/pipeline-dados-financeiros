-- ================================================================
-- 04 - JOINS E RELACIONAMENTOS
-- Pipeline de Dados Financeiros
--
-- CONCEITO DE ENGENHARIA DE DADOS:
-- Na maioria dos pipelines, os dados chegam fragmentados em
-- várias tabelas ou arquivos. O papel do engenheiro de dados
-- é cruzar essas fontes para gerar uma visão unificada —
-- o que chamamos de "camada integrada" ou "camada Silver"
-- na arquitetura Medallion (Bronze → Silver → Gold).
-- ================================================================

USE banco_digital;


-- -----------------------------------------------
-- 1. Visão completa: cliente + conta + saldo
--    (camada Silver — dados integrados)
-- -----------------------------------------------
SELECT
    cl.nome              AS cliente,
    cl.cidade,
    cl.estado,
    c.tipo_conta,
    c.numero_conta,
    c.saldo,
    c.status             AS status_conta
FROM clientes cl
INNER JOIN contas c ON cl.id_cliente = c.id_cliente
ORDER BY c.saldo DESC;


-- -----------------------------------------------
-- 2. Extrato completo de transações
--    cliente + conta + categoria + transação
-- -----------------------------------------------
SELECT
    cl.nome              AS cliente,
    c.numero_conta,
    ct.nome              AS categoria,
    t.tipo,
    t.valor,
    t.data_hora,
    t.descricao,
    t.cidade_origem,
    t.suspeita_fraude
FROM transacoes t
INNER JOIN contas                c  ON t.id_conta     = c.id_conta
INNER JOIN clientes              cl ON c.id_cliente   = cl.id_cliente
INNER JOIN categorias_transacao  ct ON t.id_categoria = ct.id_categoria
ORDER BY t.data_hora DESC;


-- -----------------------------------------------
-- 3. Clientes com cartão e seu limite disponível
-- -----------------------------------------------
SELECT
    cl.nome              AS cliente,
    c.numero_conta,
    ca.bandeira,
    ca.numero_final,
    ca.limite,
    ca.status            AS status_cartao
FROM clientes cl
INNER JOIN contas   c  ON cl.id_cliente = c.id_cliente
INNER JOIN cartoes  ca ON c.id_conta    = ca.id_conta
ORDER BY ca.limite DESC;


-- -----------------------------------------------
-- 4. LEFT JOIN: clientes SEM cartão cadastrado
--    Identifica lacunas nos dados (Data Quality)
-- -----------------------------------------------
SELECT
    cl.nome         AS cliente,
    c.numero_conta,
    ca.id_cartao    AS cartao  -- será NULL se não tiver cartão
FROM clientes cl
INNER JOIN contas  c  ON cl.id_cliente = c.id_cliente
LEFT  JOIN cartoes ca ON c.id_conta    = ca.id_conta
WHERE ca.id_cartao IS NULL;  -- filtra quem NÃO tem cartão


-- -----------------------------------------------
-- 5. Transações suspeitas com dados completos do cliente
--    (relatório para time de risco)
-- -----------------------------------------------
SELECT
    cl.nome              AS cliente,
    cl.cpf,
    c.numero_conta,
    t.tipo,
    t.valor,
    t.data_hora,
    t.descricao,
    t.cidade_origem,
    cl.cidade            AS cidade_cadastro
FROM transacoes t
INNER JOIN contas    c  ON t.id_conta   = c.id_conta
INNER JOIN clientes  cl ON c.id_cliente = cl.id_cliente
WHERE t.suspeita_fraude = TRUE
ORDER BY t.valor DESC;
-- Perceba: cidade_origem diferente de cidade_cadastro = sinal de alerta!
