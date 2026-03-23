-- ================================================================
-- 02 - CARGA DE DADOS (Data Ingestion)
-- Pipeline de Dados Financeiros
--
-- CONCEITO DE ENGENHARIA DE DADOS:
-- A ingestão é a primeira etapa de qualquer pipeline ETL/ELT.
-- Aqui simulamos uma carga batch de dados — como se esses
-- registros viessem de um sistema legado ou de uma API.
-- Em produção, ferramentas como Apache Airflow, dbt ou
-- AWS Glue automatizam esse processo.
-- ================================================================

USE banco_digital;


-- Categorias de transação (tabela dimensão)
INSERT INTO categorias_transacao (nome, tipo) VALUES
    ('Salário',          'receita'),
    ('Transferência',    'receita'),
    ('Alimentação',      'despesa'),
    ('Transporte',       'despesa'),
    ('Moradia',          'despesa'),
    ('Saúde',            'despesa'),
    ('Lazer',            'despesa'),
    ('Compras Online',   'despesa'),
    ('Saque',            'despesa'),
    ('Investimento',     'despesa');


-- Clientes fictícios (perfis variados de renda)
INSERT INTO clientes (nome, cpf, data_nasc, cidade, estado, renda_mensal, data_cadastro) VALUES
    ('Ana Paula Rodrigues',  '111.222.333-01', '1990-04-12', 'São Paulo',        'SP', 5800.00,  '2020-01-15'),
    ('Bruno Ferreira Lima',  '111.222.333-02', '1985-09-23', 'Rio de Janeiro',   'RJ', 12000.00, '2019-06-01'),
    ('Carla Mendes Silva',   '111.222.333-03', '1998-02-07', 'Embu das Artes',   'SP', 2200.00,  '2022-03-10'),
    ('Diego Costa Alves',    '111.222.333-04', '1975-11-30', 'Campinas',         'SP', 18500.00, '2018-08-20'),
    ('Elaine Souza Pinto',   '111.222.333-05', '2000-07-15', 'Salvador',         'BA', 1800.00,  '2023-01-05'),
    ('Felipe Nunes Barros',  '111.222.333-06', '1992-05-03', 'Curitiba',         'PR', 7200.00,  '2021-05-18'),
    ('Gabi Oliveira Torres', '111.222.333-07', '1988-12-19', 'Porto Alegre',     'RS', 9500.00,  '2020-09-30'),
    ('Henrique Matos Cruz',  '111.222.333-08', '1995-08-25', 'Belo Horizonte',   'MG', 3400.00,  '2022-11-12');


-- Contas bancárias (alguns clientes têm conta corrente + poupança)
INSERT INTO contas (id_cliente, tipo_conta, agencia, numero_conta, saldo, data_abertura) VALUES
    (1, 'corrente',  '0001', '00001-1', 3200.00,  '2020-01-15'),
    (2, 'corrente',  '0001', '00002-2', 18700.00, '2019-06-01'),
    (2, 'poupanca',  '0001', '00002-3', 5000.00,  '2019-06-01'),
    (3, 'corrente',  '0002', '00003-4', 420.00,   '2022-03-10'),
    (4, 'corrente',  '0002', '00004-5', 62000.00, '2018-08-20'),
    (4, 'poupanca',  '0002', '00004-6', 30000.00, '2018-08-20'),
    (5, 'corrente',  '0003', '00005-7', 180.00,   '2023-01-05'),
    (6, 'corrente',  '0003', '00006-8', 4100.00,  '2021-05-18'),
    (7, 'corrente',  '0001', '00007-9', 9800.00,  '2020-09-30'),
    (8, 'corrente',  '0002', '00008-0', 1600.00,  '2022-11-12');


-- Cartões
INSERT INTO cartoes (id_conta, numero_final, bandeira, limite) VALUES
    (1,  '4321', 'Visa',       3000.00),
    (2,  '8765', 'Mastercard', 15000.00),
    (4,  '1122', 'Visa',       30000.00),
    (6,  '3344', 'Elo',        5000.00),
    (7,  '5566', 'Mastercard', 10000.00),
    (9,  '7788', 'Visa',       8000.00),
    (10, '9900', 'Elo',        2000.00);


-- Transações — mistura de comportamentos normais e suspeitos
INSERT INTO transacoes (id_conta, id_categoria, tipo, valor, data_hora, descricao, cidade_origem, suspeita_fraude) VALUES
    -- Ana (conta 1) — comportamento normal
    (1, 1,  'credito', 5800.00, '2025-01-05 08:00:00', 'Salário Janeiro',         'São Paulo',      FALSE),
    (1, 3,  'debito',   320.00, '2025-01-08 12:30:00', 'Mercado Extra',           'São Paulo',      FALSE),
    (1, 4,  'debito',    98.00, '2025-01-10 07:45:00', 'Uber',                    'São Paulo',      FALSE),
    (1, 5,  'debito',  1200.00, '2025-01-12 09:00:00', 'Aluguel',                 'São Paulo',      FALSE),
    (1, 7,  'debito',   250.00, '2025-01-20 20:00:00', 'Restaurante',             'São Paulo',      FALSE),
    (1, 1,  'credito', 5800.00, '2025-02-05 08:00:00', 'Salário Fevereiro',       'São Paulo',      FALSE),

    -- Bruno (conta 2) — alta renda, movimentações maiores
    (2, 1,  'credito',12000.00, '2025-01-05 09:00:00', 'Salário Janeiro',         'Rio de Janeiro', FALSE),
    (2, 8,  'debito',  3500.00, '2025-01-15 14:00:00', 'Apple Store',             'Rio de Janeiro', FALSE),
    (2, 10, 'debito',  5000.00, '2025-01-18 10:30:00', 'Aporte investimento',     'Rio de Janeiro', FALSE),
    (2, 3,  'debito',   480.00, '2025-01-22 13:00:00', 'Supermercado Zona Sul',   'Rio de Janeiro', FALSE),

    -- Carla (conta 4) — baixa renda, fim do mês no vermelho
    (4, 1,  'credito', 2200.00, '2025-01-05 08:00:00', 'Salário Janeiro',         'Embu das Artes', FALSE),
    (4, 5,  'debito',   900.00, '2025-01-06 09:00:00', 'Aluguel',                 'Embu das Artes', FALSE),
    (4, 3,  'debito',   180.00, '2025-01-14 18:00:00', 'Feira do bairro',         'Embu das Artes', FALSE),
    (4, 4,  'debito',   150.00, '2025-01-20 08:00:00', 'Bilhete único mensal',    'São Paulo',      FALSE),
    (4, 9,  'debito',   200.00, '2025-01-28 17:00:00', 'Saque lotérica',          'Embu das Artes', FALSE),

    -- Diego (conta 5) — cliente VIP
    (5, 1,  'credito',18500.00, '2025-01-05 08:00:00', 'Salário Janeiro',         'Campinas',       FALSE),
    (5, 10, 'debito', 10000.00, '2025-01-06 10:00:00', 'Fundo de investimento',   'Campinas',       FALSE),
    (5, 8,  'debito',  2800.00, '2025-01-10 16:00:00', 'Shopping Campinas',       'Campinas',       FALSE),

    -- ⚠️ TRANSAÇÕES SUSPEITAS — padrões de fraude
    -- Padrão 1: vários saques pequenos em sequência (smurfing)
    (7, 9,  'saque',    490.00, '2025-01-25 22:01:00', 'Saque ATM',               'Salvador',       TRUE),
    (7, 9,  'saque',    490.00, '2025-01-25 22:04:00', 'Saque ATM',               'Salvador',       TRUE),
    (7, 9,  'saque',    490.00, '2025-01-25 22:07:00', 'Saque ATM',               'Salvador',       TRUE),

    -- Padrão 2: transação de alto valor fora da cidade habitual
    (2, 8,  'debito',  8900.00, '2025-01-30 03:22:00', 'Compra internacional',    'Miami',          TRUE),

    -- Padrão 3: movimentação noturna de valor alto
    (10, 2, 'pix',     4500.00, '2025-02-03 02:48:00', 'PIX para conta externa',  'Belo Horizonte', TRUE),

    -- Transações normais continuando
    (9, 1,  'credito', 9500.00, '2025-02-05 08:00:00', 'Salário Fevereiro',       'Porto Alegre',   FALSE),
    (9, 7,  'debito',   380.00, '2025-02-10 20:00:00', 'Cinema e jantar',         'Porto Alegre',   FALSE),
    (10, 3, 'debito',   210.00, '2025-02-12 12:00:00', 'Supermercado BH',         'Belo Horizonte', FALSE);
