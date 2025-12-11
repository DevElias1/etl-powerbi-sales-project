# Código completo para leitura de arquivos + correção da coluna Data

import os
import pandas as pd
import xml.etree.ElementTree as ET

# Pasta onde o usuário coloca os arquivos
PASTA = "dados_entrada/"

# Criar a pasta automaticamente caso ela não exista
if not os.path.exists(PASTA):
    os.makedirs(PASTA)
    print(f"📁 Pasta criada automaticamente: {PASTA}")
    print("⚠ Coloque seus arquivos dentro dela e execute novamente o script.\n")

 
# FUNÇÃO: Ler arquivo por extensão

def ler_arquivo(caminho):
    extensao = caminho.split(".")[-1].lower()

    try:
        if extensao in ["xlsx", "xls"]:
            return pd.read_excel(caminho)

        elif extensao == "csv":
            return pd.read_csv(caminho)

        elif extensao == "json":
            return pd.read_json(caminho)

        elif extensao == "txt":
            # tenta separador ; ou ,
            try:
                return pd.read_csv(caminho, sep=";")
            except:
                return pd.read_csv(caminho, sep=",")

        elif extensao == "xml":
            tree = ET.parse(caminho)
            root = tree.getroot()

            data = []
            for child in root:
                linha = {}
                for elem in child:
                    linha[elem.tag] = elem.text
                data.append(linha)

            return pd.DataFrame(data)

        else:
            print(f"⚠ Formato não suportado: {caminho}")
            return None

    except Exception as e:
        print(f"❌ Erro ao abrir {caminho}: {e}")
        return None


# FUNÇÃO: Carregar todos os arquivos da pasta

def carregar_arquivos():
    arquivos = os.listdir(PASTA)
    tabelas = {}

    if not arquivos:
        print("⚠ Nenhum arquivo encontrado na pasta dados_entrada/")
        print("➡ Coloque seus arquivos e execute novamente.")
        return {}

    print("🔍 Procurando arquivos na pasta dados_entrada...\n")

    for arquivo in arquivos:
        caminho = os.path.join(PASTA, arquivo)

        if not os.path.isfile(caminho):
            continue

        print(f"➡ Lendo arquivo: {arquivo}")
        df = ler_arquivo(caminho)

        if df is not None:
            nome = arquivo.split(".")[0]  # nome da tabela
            tabelas[nome] = df
            print(f"   ✅ Arquivo carregado: {arquivo} → DataFrame '{nome}'\n")

    return tabelas


# EXECUÇÃO PRINCIPAL

tabelas = carregar_arquivos()

if tabelas:
    print("\n📌 DATAFRAMES CARREGADOS:")
    for nome, df in tabelas.items():
        print(f"\n--- {nome.upper()} ---")
        print(df.head())
        print("\nInformações:")
        print(df.info())
        print("\nValores faltantes:")
        print(df.isnull().sum())


# ------------------------------------------------------------
# EXPLICAÇÃO DETALHADA (COMENTÁRIOS) — Cole isto abaixo do seu código
# ------------------------------------------------------------
#
# Resumo rápido:
# Este arquivo tem 3 responsabilidades principais:
#  1) Criar/verificar a pasta de entrada (dados_entrada/)
#  2) Varredura dessa pasta e leitura automática de arquivos por extensão
#  3) Expor os DataFrames carregados para uso posterior no ETL
#
# Abaixo, linha a linha / bloco a bloco: o que cada biblioteca e trecho faz,
# por que está ali, alternativas e boas práticas.
#
# ------------------------------------------------------------
# BIBLIOTECAS USADAS
# ------------------------------------------------------------
# import os
#   - Biblioteca padrão do Python para interagir com o sistema operacional.
#   - Usamos para listar arquivos (os.listdir), juntar caminhos (os.path.join),
#     criar pastas (os.makedirs) e verificar "isfile" e existência de diretórios.
#   - Alternativas/observações: manter, pois é a forma correta e portátil.
#
# import pandas as pd
#   - Pandas é a biblioteca principal de manipulação de dados em Python.
#   - Fornece DataFrame: estrutura tabular (linhas x colunas) muito usada para ETL.
#   - Funções importantes usadas: read_excel, read_csv, read_json, DataFrame(...)
#   - Observações:
#       * Para Excel, pandas usa engines (ex.: "openpyxl") por trás — se tiver erro, instale
#         openpyxl (pip install openpyxl).
#       * Para arquivos muito grandes (> memória), considerar Dask, Vaex, ou Spark.
#
# import xml.etree.ElementTree as ET
#   - Biblioteca padrão para parse de XML.
#   - Aqui usamos para transformar um XML simples em uma lista de dicionários e criar DataFrame.
#   - Observações:
#       * Para XMLs grandes/complexos, usar lxml (mais rápido e robusto).
#       * A estrutura do XML precisa ser regular para virar tabela facilmente.
#
# ------------------------------------------------------------
# VARIÁVEIS E CONFIGURAÇÃO
# ------------------------------------------------------------
# PASTA = "dados_entrada/"
#   - Caminho relativo onde o usuário joga os arquivos.
#   - Pode ser absoluto se preferir (ex.: r"C:\meu_projeto\dados_entrada").
#   - Boa prática: configurar via variável de ambiente ou CLI/argparse para flexibilidade.
#
# Criação automática da pasta:
#   - if not os.path.exists(PASTA): os.makedirs(PASTA)
#   - Evita FileNotFoundError e melhora UX: o script cria a pasta e pede para o usuário colocar arquivos.
#
# ------------------------------------------------------------
# FUNÇÃO ler_arquivo(caminho)
# ------------------------------------------------------------
# Objetivo:
#   - Receber um caminho de arquivo e retornar um pandas.DataFrame com os dados.
#
# Como ela identifica o formato:
#   - extensao = caminho.split(".")[-1].lower()
#   - Simples e eficaz, mas atenção: arquivos sem extensão ou com múltiplos pontos podem confundir.
#
# Suporte no código:
#   - Excel: extensao in ["xlsx", "xls"] -> pd.read_excel(caminho)
#       * pd.read_excel lida com múltiplas sheets (por padrão lê a primeira); para ler sheet específica:
#         pd.read_excel(caminho, sheet_name="NomeDaPlanilha")
#       * Se precisar performace, considerar engine openpyxl (para xlsx) ou xlrd (antigo).
#
#   - CSV: pd.read_csv(caminho)
#       * Suporta parâmetros: sep, encoding, parse_dates, dtype, etc.
#       * Atenção com encoding (utf-8 vs latin-1). Se der erro, tente encoding="latin-1".
#
#   - JSON: pd.read_json(caminho)
#       * JSON tabular direto funciona; JSON aninhado pode precisar de json_normalize.
#
#   - TXT: tentamos dois separadores (;) e depois (,)
#       * Boa heurística para arquivos .txt que na prática são CSVs com separadores variados.
#       * Pode-se estender para detectar separador automaticamente (ex.: csv.Sniffer).
#
#   - XML: usamos xml.etree.ElementTree para parsear e extrair elementos em lista de dicionários
#       * Depois transformamos em pd.DataFrame(data)
#       * Limitação: funciona melhor com XML estruturado como uma lista de registros.
#
# Tratamento de erros:
#   - try/except envolvendo cada leitura para capturar problemas e não quebrar todo o loop.
#   - Retorna None para formatos não suportados ou em caso de erro, permitindo o script continuar.
#
# Avisos:
#   - Arquivos binários ou formatos complexos (parquet, avro, parquet via pyarrow) não estão no script.
#   - Para parquet, usar pd.read_parquet(caminho) e instalar pyarrow/fastparquet.
#
# ------------------------------------------------------------
# FUNÇÃO carregar_arquivos()
# ------------------------------------------------------------
# Objetivo:
#   - Listar todos os arquivos em PASTA, chamar ler_arquivo em cada um e montar um dicionário
#     {nome_base_do_arquivo: DataFrame}.
#
# Lógica:
#   - arquivos = os.listdir(PASTA)
#   - testar se a lista está vazia e avisar o usuário
#   - iterar, montar caminho absoluto com os.path.join(PASTA, arquivo)
#   - pular entradas que não são arquivos (pastas)
#   - para cada DataFrame carregado, usar o nome base (arquivo.split(".")[0]) como chave
#
# Observações:
#   - Se houver dois arquivos com mesmo nome base mas diferentes extensões (ex.: vendas.csv e vendas.xlsx),
#     o dicionário vai sobrescrever uma entrada com a outra. Se isso for um risco, podemos:
#       * Usar chave completa com extensão, ou
#       * Agregar em lista (ex.: tabelas.setdefault(nome_base, []).append(df))
#
# ------------------------------------------------------------
# PARTE DE EXIBIÇÃO/DEBUG
# ------------------------------------------------------------
# Após carregar, o script imprime:
#   - head() de cada DataFrame
#   - df.info()
#   - df.isnull().sum()
#
# Isso serve como inspeção inicial (quick check) antes do ETL.
# Em produção, você normalmente:
#   - gera logs (arquivo de log) em vez de print()
#   - gera métricas (nº de linhas, colunas, erros) para monitoramento
#
# ------------------------------------------------------------
# BOAS PRÁTICAS / PRÓXIMOS PASSOS SUGERIDOS
# ------------------------------------------------------------
# 1) Logging:
#    - Substituir prints por logging (módulo logging), com níveis (INFO, WARNING, ERROR).
#    - Salvar em arquivo de logs para auditoria.
#
# 2) Configuração:
#    - Receber PASTA via argumento de linha de comando (argparse) ou variáveis de ambiente.
#
# 3) Identificação automática de tabelas (quando nomes variam):
#    - Se arquivos são variáveis, implementar heurística de identificação (ex.: buscar colunas que indicam "vendas",
#      procurar colunas como "valor", "data", "produto", "id_financeiro").
#
# 4) Tratamento de grandes volumes:
#    - Para arquivos que não cabem na memória, considerar:
#       * Dask (API compatível com pandas), ou
#       * PySpark (pyspark.sql), ou
#       * Ler em chunks com pd.read_csv(..., chunksize=...)
#
# 5) Formatos adicionais:
#    - Parquet: pd.read_parquet (muito usado em data engineering por performance)
#    - Feather: pd.read_feather (rápido, colunar)
#    - Bancos SQL: pd.read_sql(query, connection)
#
# 6) Validações e testes:
#    - Validar schema esperado (colunas obrigatórias, tipos)
#    - Validar integridade (IDs únicos, chaves estrangeiras)
#    - Implementar testes unitários (pytest) para suas funções ETL
#
# 7) Orquestração:
#    - Em projetos maiores, orquestrar com Airflow, Prefect ou Dagster
#    - dbt é excelente para transformações SQL em Data Warehouses (ex.: BigQuery/Redshift)
#
# 8) Segurança:
#    - Cuidado com arquivos vindos de terceiros (injeção, formatos maliciosos)
#    - Evitar executar código vindo de arquivos (ex.: eval)
#
# 9) Documentação:
#    - Documente o contrato dos arquivos (colunas esperadas, tipos, frequência de atualização)
#
# ------------------------------------------------------------
# DICAS PRÁTICAS PARA VOCÊ (estudante / 1º projeto)
# ------------------------------------------------------------
# - Separar ingestão (este script) e transformações (seu ETL) é ótima prática: modularidade.
# - Sempre rode o script com amostras dos dados primeiro para prevenir surpresas.
# - Use um notebook (Jupyter) para explorar os dados e validar transformações antes de automatizar.
# - Quando pedir ajuda (IA, colegas), sempre traga prints/erros e descreva o que tentou.
#
# ------------------------------------------------------------
# EXEMPLO RÁPIDO DE COMO EVOLUIR (após este script)
# ------------------------------------------------------------
# - Adicione uma função `identificar_tabelas(tabelas)` que mapeia cada DataFrame para o papel
#   (ex.: "vendas", "financeiro", "clientes") usando heurísticas de colunas.
# - Crie `etl_padronizacao(df)` que recebe um DF e aplica limpeza padrão (strip, lower, trim, converter tipos).
# - Crie `etl_transformacoes(tabelas)` que combina DataFrames e gera o dataset final.
#
# ------------------------------------------------------------
# CONCLUSÃO:
# ------------------------------------------------------------
# O código que você criou já tem a estrutura básica correta e profissional:
# - modular (funções separadas)
# - tolerante a erros (try/except)
# - pronto para extensão (mais formatos, logging, validações)
#
# Cole esses comentários abaixo do seu script para referência enquanto trabalhamos no ETL
# e, quando quiser, eu já posso começar a escrever as 5 transformações seguindo as melhores práticas.
#
# Boa! Vamos para o ETL quando você disser. 🚀
#
# ------------------------------------------------------------
