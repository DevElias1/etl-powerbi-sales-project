import numpy as np
import pandas as pd

# 1) CARREGAR PLANILHAS ORIGINAIS

df_vendas = pd.read_excel("Vendas.xlsx", engine="openpyxl")
df_fin = pd.read_excel("Financeiro.xlsx", engine="openpyxl")

# Limpeza
df_vendas.dropna(inplace=True)
df_fin.dropna(inplace=True)

# Tipos de dados
df_vendas["Data"] = pd.to_datetime(df_vendas["Data"])
df_fin["Mes"] = pd.to_datetime(df_fin["Mes"], format="%Y-%m", errors="coerce")

# Criar Receita se não existir
if "Receita" not in df_vendas.columns:
    df_vendas["Receita"] = df_vendas["Quantidade"] * df_vendas["Preco_Unit"]


# 2) ETL NORMAL — AGRUPAÇÕES

# Total de vendas por mês
vendas_mensais = (
    df_vendas
    .groupby(df_vendas["Data"].dt.to_period("M"))["Receita"]
    .sum()
    .reset_index()
)

vendas_mensais.rename(columns={"Receita": "Total_Vendas"}, inplace=True)
vendas_mensais.rename(columns={vendas_mensais.columns[0]: "Periodo"}, inplace=True)
vendas_mensais["Periodo"] = vendas_mensais["Periodo"].dt.to_timestamp()

# Financeiro por mês
financeiro_mensal = (
    df_fin
    .groupby([df_fin["Mes"].dt.to_period("M"), "Tipo"])["Valor"]
    .sum()
    .reset_index()
)

financeiro_mensal = financeiro_mensal.pivot(
    index="Mes", columns="Tipo", values="Valor"
).fillna(0).reset_index()

financeiro_mensal["Mes"] = financeiro_mensal["Mes"].dt.to_timestamp()

if "Receita" not in financeiro_mensal.columns:
    financeiro_mensal["Receita"] = 0

if "Despesa" not in financeiro_mensal.columns:
    financeiro_mensal["Despesa"] = 0


# 3) MERGE FINAL (ANTES DA EXPANSÃO)

df_merged = pd.merge(
    vendas_mensais,
    financeiro_mensal,
    left_on="Periodo",
    right_on="Mes",
    how="left"
)

df_merged["Lucro_Estimado"] = df_merged["Total_Vendas"] - df_merged["Despesa"]

df_merged.drop(columns=["Mes"], inplace=True)


# 4) EXPANSÃO AUTOMÁTICA — MIN 30 LINHAS (RESUMO_MENSAL)

while len(df_merged) < 30:

    ultima = df_merged.iloc[-1]

    novo_periodo = ultima["Periodo"] + pd.DateOffset(months=1)

    novo_total_vendas = ultima["Total_Vendas"] * np.random.uniform(0.90, 1.15)
    nova_receita = ultima["Receita"] * np.random.uniform(0.90, 1.15)
    nova_despesa = ultima["Despesa"] * np.random.uniform(0.90, 1.15)

    novo_lucro = novo_total_vendas - nova_despesa

    nova_linha = pd.DataFrame({
        "Periodo": [novo_periodo],
        "Total_Vendas": [novo_total_vendas],
        "Receita": [nova_receita],
        "Despesa": [nova_despesa],
        "Lucro_Estimado": [novo_lucro]
    })

    df_merged = pd.concat([df_merged, nova_linha], ignore_index=True)


# 5) RANKING DE PRODUTOS — EXPANDIR P/ 30 LINHAS

top_produtos = (
    df_vendas.groupby("Produto")["Receita"]
    .sum()
    .sort_values(ascending=False)
    .reset_index()
)

# Criar produtos sintéticos até chegar a 30
while len(top_produtos) < 30:
    base = top_produtos.sample(1).iloc[0]
    
    novo_produto = base["Produto"] + "_Extra_" + str(len(top_produtos) + 1)
    nova_receita = base["Receita"] * np.random.uniform(0.85, 1.20)

    nova_linha = pd.DataFrame({
        "Produto": [novo_produto],
        "Receita": [nova_receita]
    })

    top_produtos = pd.concat([top_produtos, nova_linha], ignore_index=True)

# Ordenar novamente
top_produtos = top_produtos.sort_values("Receita", ascending=False).reset_index(drop=True)

# 6) EXPORTAÇÃO FINAL

df_merged.to_excel("Resumo_Mensal.xlsx", index=False, engine="openpyxl")
top_produtos.to_excel("Top_Produtos.xlsx", index=False, engine="openpyxl")

print("\n📊 ETL concluído com sucesso!")
print("Arquivos gerados:")
print("➡ Resumo_Mensal.xlsx (30+ linhas garantidas)")
print("➡ Top_Produtos.xlsx (30+ produtos garantidos)\n")


# =========================================================
# EXPLICAÇÃO DO CÓDIGO — ETAPA POR ETAPA
# =========================================================

# 1) IMPORTAÇÃO DE BIBLIOTECAS
# - numpy: usado para gerar valores aleatórios (na expansão dos dados)
# - pandas: biblioteca principal para ETL, leitura de Excel, agrupamentos e transformações

# 2) LEITURA DOS ARQUIVOS
# - pd.read_excel(): carrega os arquivos Excel para DataFrames
# - engine="openpyxl": garante compatibilidade com arquivos .xlsx
# - dropna(): remove linhas com valores vazios ou incompletos

# 3) TRATAMENTO DE TIPOS
# - pd.to_datetime(): converte colunas para formato de data
# - errors="coerce": converte erros em NaT (evita travamentos)
# - criação da coluna "Receita" caso não exista:
#   Quantidade * Preço_Unitário

# 4) AGRUPAMENTO DE DADOS
# - dt.to_period("M"): transforma as datas em períodos mensais
# - groupby(): agrupa por mês e soma a receita
# - rename(): renomeia colunas para padronização
# - dt.to_timestamp(): converte Period → datetime normal (compatível com merges)

# 5) TRATAMENTO DO FINANCEIRO
# - Agrupa Financeiro por mês e tipo (Receita / Despesa)
# - pivot(): transforma as categorias "Tipo" em colunas
# - fillna(): qualquer valor faltante vira zero
# - Garantimos as colunas Receita e Despesa mesmo que não existam no arquivo

# 6) MERGE DOS DATASETS
# - pd.merge(): junta vendas mês a mês com receitas e despesas do financeiro
# - left_on="Periodo": usa o mês vindo das vendas
# - right_on="Mes": usa o mês vindo do financeiro
# - how="left": mantém todos os meses das vendas
# - Criação do Lucro_Estimado = Total_Vendas - Despesa

# 7) EXPANSÃO AUTOMÁTICA DO DATASET (MÍNIMO 30 LINHAS)
# Esta parte foi criada porque seu arquivo original tinha poucos meses.
# O objetivo é garantir que o Power BI tenha volume de dados suficiente.
#
# A lógica funciona assim:
# - Pega a última linha existente
# - Avança 1 mês
# - Gera valores novos baseados em uma variação percentual realista:
#       Total_Vendas   → ±15%
#       Receita        → ±15%
#       Despesa        → ±15%
# - Calcula lucro
# - Adiciona a nova linha ao arquivo final
# - Repete até chegar a 30 linhas

# 8) RANKING DE PRODUTOS (MÍNIMO 30 PRODUTOS)
# - Soma a receita total por produto
# - Ordena do maior para o menor
# - Se houver menos de 30 produtos:
#       → cria produtos fictícios baseados em produtos reais
#       → "Produto_Extra_x"
#       → receita varia entre 85% e 120% do produto de referência
# - Reordena tudo novamente

# 9) EXPORTAÇÃO DOS ARQUIVOS FINAIS
# - to_excel(): salva como Excel
# - index=False: remove a coluna de índice
# - Engine openpyxl: garante compatibilidade
# Arquivos gerados:
#   → Resumo_Mensal.xlsx   (mínimo 30 meses)
#   → Top_Produtos.xlsx    (mínimo 30 produtos)

# 10) MENSAGEM FINAL
# - Apenas indica que o processo ETL terminou corretamente.
