# 🏦 Pipeline de Dados Financeiros — SQL para Engenharia de Dados

Projeto de estudo que simula um **pipeline de dados de um banco digital**, com modelagem relacional, consultas analíticas e boas práticas de Engenharia de Dados.

> Desenvolvido por **Giovanna de Souza Pereira** como parte da jornada de aprendizado em Engenharia de Dados.

---

## 💡 Contexto

Bancos e fintechs lidam diariamente com milhões de transações. Por trás disso tudo existe uma camada de **Engenharia de Dados** responsável por:

- Modelar o banco de dados de forma eficiente
- Criar pipelines que transformam dados brutos em informação
- Detectar padrões suspeitos (fraudes)
- Gerar relatórios para times de negócio e compliance

Este projeto simula esse ambiente com dados fictícios e consultas progressivas — do básico ao analítico.

---

## 🗂️ Estrutura do Projeto

```
pipeline-dados-financeiros/
│
├── README.md
├── 01_modelagem.sql              # Criação do banco e tabelas (Data Modeling)
├── 02_carga_dados.sql            # Inserção de dados fictícios (Data Ingestion)
├── 03_consultas_basicas.sql      # SELECTs, filtros, ordenação
├── 04_joins_e_relacionamentos.sql # JOINs entre tabelas (o coração do SQL)
├── 05_agregacoes_e_kpis.sql      # KPIs financeiros com GROUP BY
├── 06_deteccao_anomalias.sql     # Detecção de padrões suspeitos
└── 07_pipeline_analitico.sql     # Camada analítica — visão de negócio completa
```

---

## 🧱 Modelagem do Banco (Data Modeling)

```
clientes ──────────< contas ──────────< transacoes
                        │
                        └──────────< cartoes
```

| Tabela | Descrição |
|---|---|
| `clientes` | Cadastro de correntistas |
| `contas` | Contas correntes e poupança |
| `transacoes` | Movimentações financeiras (débito/crédito/pix/ted) |
| `cartoes` | Cartões vinculados às contas |
| `categorias_transacao` | Lookup de categorias (alimentação, transporte...) |

---

## 🛠️ Tecnologias

- **MySQL** — banco de dados relacional
- **SQL** — modelagem, ingestão e análise
- Compatível com **PostgreSQL** e **SQLite** (pequenos ajustes de sintaxe)

---

## 🚀 Como executar

1. Use o [DB Fiddle](https://www.db-fiddle.com/) (online, sem instalar nada) ou instale o MySQL
2. Execute os arquivos **na ordem numérica**
3. Cada arquivo tem comentários explicando o conceito aplicado

---

## 📚 Conceitos de Engenharia de Dados praticados

- ✅ Modelagem relacional com chaves estrangeiras
- ✅ Ingestão de dados (simulação de carga batch)
- ✅ Transformações com SQL (ETL básico)
- ✅ KPIs financeiros com funções de agregação
- ✅ Detecção de anomalias com subqueries
- ✅ Camada analítica (equivalente a uma tabela de Data Warehouse)

---

## 👩‍💻 Autora

**Giovanna de Souza Pereira**
Estudante de Tecnólogo em Banco de Dados — UNASP
Aprendiz no Banco Bradesco | Em transição para Engenharia de Dados

[![LinkedIn](https://img.shields.io/badge/LinkedIn-giovannadesouzapereira-blue)](https://linkedin.com/in/giovannadesouzapereira)
