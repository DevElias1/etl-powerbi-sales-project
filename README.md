# etl-powerbi-sales-project

# 📊 Projeto de ETL em Python + Dashboard Power BI

## 🔧 Etapas do Projeto

---

## 1) 📥 Coleta e Ingestão
- Leitura automática de múltiplos arquivos (**Excel, CSV, TXT, JSON, XML**)
- Script Python genérico para ingestão em lote dos arquivos
- Padronização dos DataFrames para facilitar o processamento e garantir consistência

---

## 2) 🛠️ ETL Completo em Python (Pandas + NumPy)

### 🧹 Limpeza dos Dados
- Remoção de duplicidades e valores nulos  
- Correção de inconsistências  
- Padronização de nomes, tipos e categorias  
- Conversão de colunas de data  

### ⚡ Padronização com Timestamp
- Datas convertidas para **timestamp** para reduzir espaço e aumentar performance
- Extração otimizada de **mês** e **ano**
- Pipelines mais leves, rápidos e escaláveis

### 📊 Transformações e Métricas
- Cálculo automático de:
  - **Receita**
  - **Despesa**
  - **Lucro Estimado**
- Agrupamento por mês e produto
- Pivot das categorias financeiras (Receita / Despesa)
- Merge das tabelas usando chaves temporais otimizadas

### 📈 Expansão Artificial dos Dados
- Geração automática de novos meses até atingir **30 períodos mínimos**
- Criação de produtos sintéticos até **30 itens**
- Variações realistas baseadas em percentuais para manter coerência nos dados

---

## 3) 📤 Exportação dos Arquivos Finais
- **Resumo_Mensal.xlsx** → Faturamento, despesas e lucro estimado por mês  
- **Top_Produtos.xlsx** → Ranking completo de produtos por receita  

---

## 4) 📊 Dashboard Power BI
O dashboard apresenta:

- Total de vendas  
- Lucro estimado  
- Ranking de produtos  
- Sazonalidade mensal  
- Relação **Vendas × Lucro**  
- Picos e quedas de desempenho  

---

## ✔️ Tecnologias Utilizadas
- **Python:** Pandas, NumPy  
- **Excel:** arquivos transformados  
- **Power BI:** visualização e insights  
- **GitHub:** versionamento e documentação  

---

## 🎯 Objetivo Geral
Criar um pipeline completo de dados, com tratamento profissional, otimização via timestamp e visualização executiva para tomada de decisão no Power BI.

