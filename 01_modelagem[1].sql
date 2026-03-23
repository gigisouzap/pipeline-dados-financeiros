-- ================================================================
-- 01 - MODELAGEM DO BANCO DE DADOS (Data Modeling)
-- Pipeline de Dados Financeiros
-- Autora: Giovanna de Souza Pereira
--
-- CONCEITO DE ENGENHARIA DE DADOS:
-- A modelagem é a base de qualquer pipeline. Aqui definimos
-- como os dados se relacionam — o que chamamos de esquema
-- relacional. Um modelo bem feito evita dados duplicados,
-- facilita consultas e garante integridade.
-- ================================================================

CREATE DATABASE IF NOT EXISTS banco_digital;
USE banco_digital;


-- ----------------------------------------------------------------
-- TABELA: categorias_transacao
-- Tabela de lookup (dicionário) — classifica cada transação
-- Em pipelines reais, esse tipo de tabela vem de uma camada
-- chamada "dimensão" no Data Warehouse.
-- ----------------------------------------------------------------
CREATE TABLE categorias_transacao (
    id_categoria  INT PRIMARY KEY AUTO_INCREMENT,
    nome          VARCHAR(80) NOT NULL,   -- ex: Alimentação, Transporte
    tipo          VARCHAR(30) NOT NULL    -- ex: despesa, receita
);


-- ----------------------------------------------------------------
-- TABELA: clientes
-- Representa os correntistas do banco.
-- Em um pipeline real, essa tabela seria alimentada por um
-- sistema de CRM via ingestão batch ou streaming (Kafka, etc).
-- ----------------------------------------------------------------
CREATE TABLE clientes (
    id_cliente    INT PRIMARY KEY AUTO_INCREMENT,
    nome          VARCHAR(150) NOT NULL,
    cpf           VARCHAR(14)  NOT NULL UNIQUE,
    data_nasc     DATE         NOT NULL,
    cidade        VARCHAR(100),
    estado        CHAR(2),
    renda_mensal  DECIMAL(12,2),
    data_cadastro DATE         NOT NULL,
    ativo         BOOLEAN      DEFAULT TRUE  -- flag de soft delete (boas práticas)
);


-- ----------------------------------------------------------------
-- TABELA: contas
-- Cada cliente pode ter mais de uma conta (corrente, poupança).
-- Relacionamento 1:N — um cliente, várias contas.
-- ----------------------------------------------------------------
CREATE TABLE contas (
    id_conta      INT PRIMARY KEY AUTO_INCREMENT,
    id_cliente    INT            NOT NULL,
    tipo_conta    VARCHAR(30)    NOT NULL,         -- corrente | poupanca
    agencia       VARCHAR(10)    NOT NULL,
    numero_conta  VARCHAR(20)    NOT NULL UNIQUE,
    saldo         DECIMAL(14,2)  DEFAULT 0.00,
    data_abertura DATE           NOT NULL,
    status        VARCHAR(20)    DEFAULT 'ativa',  -- ativa | bloqueada | encerrada
    FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente)
);


-- ----------------------------------------------------------------
-- TABELA: cartoes
-- Cartões vinculados a uma conta. Um conta pode ter vários.
-- ----------------------------------------------------------------
CREATE TABLE cartoes (
    id_cartao     INT PRIMARY KEY AUTO_INCREMENT,
    id_conta      INT           NOT NULL,
    numero_final  CHAR(4)       NOT NULL,  -- armazenamos só os 4 últimos (segurança!)
    bandeira      VARCHAR(30),             -- Visa, Mastercard, Elo...
    limite        DECIMAL(12,2),
    status        VARCHAR(20)   DEFAULT 'ativo',
    FOREIGN KEY (id_conta) REFERENCES contas(id_conta)
);


-- ----------------------------------------------------------------
-- TABELA: transacoes
-- Coração do pipeline — registra cada movimentação financeira.
-- Em produção, essa tabela recebe milhões de linhas por dia.
--
-- BOAS PRÁTICAS aplicadas aqui:
--   - valor sempre positivo (o tipo indica débito/crédito)
--   - id_categoria como FK para evitar texto livre
--   - campo "suspeita_fraude" para marcar anomalias
-- ----------------------------------------------------------------
CREATE TABLE transacoes (
    id_transacao    INT PRIMARY KEY AUTO_INCREMENT,
    id_conta        INT           NOT NULL,
    id_categoria    INT,
    tipo            VARCHAR(20)   NOT NULL,  -- debito | credito | pix | ted | saque
    valor           DECIMAL(14,2) NOT NULL,
    data_hora       DATETIME      NOT NULL,
    descricao       VARCHAR(200),
    cidade_origem   VARCHAR(100),
    suspeita_fraude BOOLEAN       DEFAULT FALSE,  -- flag para análise posterior
    FOREIGN KEY (id_conta)     REFERENCES contas(id_conta),
    FOREIGN KEY (id_categoria) REFERENCES categorias_transacao(id_categoria)
);
