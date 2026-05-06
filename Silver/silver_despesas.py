# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Silver - Fato Despesas e Dimensao Fornecedores
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook transforma os dados brutos de despesas CEAP (Bronze) em modelo dimensional:
# MAGIC um fato de despesas (fato_despesas) e uma dimensao de fornecedores (dim_fornecedores).
# MAGIC Transformacoes: conversao de tipos, padronizacao de CNPJ/nome, geracao de surrogate key,
# MAGIC remocao de valores zerados/negativos e deduplicacao por documento.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `dt0025_dev.ft_bronze.despesas` | Despesas brutas por deputado com todos campos da API |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_silver.fato_despesas` | Fato despesas (grain: 1 linha por documento) |
# MAGIC | `dt0025_dev.ft_silver.dim_fornecedores` | Dimensao fornecedores unicos com tipo doc fiscal |
# MAGIC
# MAGIC ## Responsavel
# MAGIC - **Ernesto Bassoli Junior**

# COMMAND ----------

# DBTITLE 1,Definicao de Funcoes e Parametros
# MAGIC %md
# MAGIC # Definicao de Funcoes, Parametros e Variaveis

# COMMAND ----------

# DBTITLE 1,Sobre o comando run
# MAGIC %md
# MAGIC Ao executar o comando `%run ../FUNCOES_GENERICAS`, o Python ira interpretar e executar
# MAGIC o conteudo do arquivo FUNCOES_GENERICAS.py, disponibilizando todas as funcoes de
# MAGIC ingestao, gravacao, controle incremental e validacao para uso neste notebook.

# COMMAND ----------

# DBTITLE 1,Importacao de Funcoes
# MAGIC %run ../FUNCOES_GENERICAS

# COMMAND ----------

# DBTITLE 1,Sobre o Registro de Log
# MAGIC %md
# MAGIC Na celula abaixo e registrado o inicio da execucao deste notebook no sistema de logs.
# MAGIC Isso permite rastrear quando foi executado, quanto tempo levou e se houve erros.

# COMMAND ----------

# DBTITLE 1,Registro de Inicio
# ============================================================
# REGISTRO DE INICIO NO LOG
# ============================================================
# Registra o inicio da execucao deste notebook na tabela
# de logs para rastreabilidade completa do pipeline.
# ============================================================

# Registra inicio no sistema de logging centralizado
log_notebook_start("silver_despesas")

# COMMAND ----------

# DBTITLE 1,Leitura dos Dados
# MAGIC %md
# MAGIC # Leitura da Camada Bronze

# COMMAND ----------

# DBTITLE 1,Sobre a Leitura
# MAGIC %md
# MAGIC Na celula abaixo e carregada a tabela de despesas brutas da camada Bronze.
# MAGIC Esta tabela contem todas as despesas de todos os deputados com campos originais da API.

# COMMAND ----------

# DBTITLE 1,Le Bronze Despesas
# ============================================================
# LEITURA DA CAMADA BRONZE
# ============================================================
# Carrega despesas brutas com todos os campos da API.
# ============================================================

# Informa o usuario
print("Lendo bronze.despesas...")

# Le a tabela Bronze de despesas
df_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.despesas")

# Exibe quantidade de registros brutos
print(f"   Registros brutos: {df_bronze.count()}")

# COMMAND ----------

# DBTITLE 1,Transformacao dos Dados
# MAGIC %md
# MAGIC # Transformacao em Modelo Dimensional

# COMMAND ----------

# DBTITLE 1,Sobre a Transformacao
# MAGIC %md
# MAGIC Na celula abaixo os dados brutos sao transformados em tabela fato com as seguintes etapas:
# MAGIC 1. **Selecao e renomeacao** de campos para nomes descritivos em portugues
# MAGIC 2. **Conversao de tipos**: valores para DECIMAL(15,2), datas para DATE
# MAGIC 3. **Padronizacao**: fornecedor em maiusculas, CNPJ sem pontuacao
# MAGIC 4. **Surrogate key**: hash MD5 de (deputado + codDocumento + numDocumento)
# MAGIC 5. **Filtros**: remove valores zerados e registros sem ID

# COMMAND ----------

# DBTITLE 1,Transforma Fato Despesas
# ============================================================
# TRANSFORMACAO EM TABELA FATO
# ============================================================
# Cria a tabela fato de despesas com campos limpos e tipados.
# ============================================================

# Informa o usuario
print("Transformando em fato_despesas...")

# Aplica transformacoes PySpark em cadeia
df_silver = (df_bronze
    # Seleciona e renomeia campos
    .select(
        # ID do deputado convertido para LONG
        col("_deputado_id").cast("long").alias("id_deputado"),
        # Ano da despesa
        col("ano").cast("int"),
        # Mes da despesa
        col("mes").cast("int"),
        # Categoria/tipo da despesa (alimentacao, passagem, etc)
        col("tipoDespesa").alias("categoria_despesa"),
        # Codigo do documento fiscal
        col("codDocumento").alias("cod_documento"),
        # Tipo do documento (nota fiscal, recibo, etc)
        col("tipoDocumento").alias("tipo_documento"),
        # Data do documento convertida para tipo DATE
        to_date(col("dataDocumento")).alias("data_documento"),
        # Numero do documento fiscal
        col("numDocumento").alias("num_documento"),
        # Valor bruto do documento em DECIMAL(15,2)
        col("valorDocumento").cast("decimal(15,2)").alias("valor_documento"),
        # Valor liquido (descontado glosas) em DECIMAL(15,2)
        col("valorLiquido").cast("decimal(15,2)").alias("valor_liquido"),
        # Valor da glosa (desconto por irregularidade)
        col("valorGlosa").cast("decimal(15,2)").alias("valor_glosa"),
        # Nome do fornecedor
        col("nomeFornecedor").alias("nome_fornecedor"),
        # CNPJ ou CPF do fornecedor
        col("cnpjCpfFornecedor").alias("cnpj_cpf_fornecedor"),
        # URL do documento digitalizado
        col("urlDocumento").alias("url_documento"),
        # Numero de ressarcimento
        col("numRessarcimento").alias("num_ressarcimento"),
        # Parcela (para despesas parceladas)
        col("parcela").cast("int")
    )
    # Remove registros sem ID de deputado
    .filter(col("id_deputado").isNotNull())
    # Remove despesas com valor zero ou negativo
    .filter(col("valor_liquido") > 0)
    # Padroniza nome do fornecedor em maiusculas
    .withColumn("nome_fornecedor", upper(trim(col("nome_fornecedor"))))
    # Remove espacos do CNPJ/CPF
    .withColumn("cnpj_cpf_fornecedor", trim(col("cnpj_cpf_fornecedor")))
    # Gera surrogate key via hash MD5 (deputado + doc + numero)
    .withColumn("sk_despesa", md5(
        concat_ws("||",
            col("id_deputado").cast("string"),
            col("cod_documento"),
            col("num_documento")
        )
    ))
    # Remove duplicatas pela combinacao unica
    .dropDuplicates(["id_deputado", "cod_documento", "num_documento"])
)

# Exibe quantidade de registros na tabela fato
print(f"   Registros fato: {df_silver.count()}")

# COMMAND ----------

# DBTITLE 1,Sobre a Dimensao Fornecedor
# MAGIC %md
# MAGIC ## Dimensao de Fornecedores
# MAGIC
# MAGIC Na celula abaixo e gerada a dimensao de fornecedores unicos extraida das despesas.
# MAGIC Cada fornecedor possui CNPJ/CPF, nome padronizado e classificacao do tipo de
# MAGIC documento fiscal (CNPJ=14 digitos, CPF=11 digitos, OUTRO=formato irregular).

# COMMAND ----------

# DBTITLE 1,Gera Dim Fornecedor
# ============================================================
# DIMENSAO FORNECEDOR (DERIVADA)
# ============================================================
# Extrai fornecedores unicos a partir das despesas.
# Classifica o tipo de documento fiscal.
# ============================================================

# Informa o usuario
print("Gerando dim_fornecedor...")

# Cria dimensao de fornecedores unicos
df_fornecedores = (df_silver
    # Seleciona apenas campos do fornecedor
    .select("cnpj_cpf_fornecedor", "nome_fornecedor")
    # Remove registros sem CNPJ/CPF
    .filter(col("cnpj_cpf_fornecedor").isNotNull())
    .filter(col("cnpj_cpf_fornecedor") != "")
    # Mantem apenas um registro por CNPJ/CPF
    .dropDuplicates(["cnpj_cpf_fornecedor"])
    # Gera surrogate key
    .withColumn("sk_fornecedor", md5(col("cnpj_cpf_fornecedor")))
    # Classifica tipo de documento fiscal
    .withColumn("tipo_documento_fiscal", 
        when(col("cnpj_cpf_fornecedor").rlike("^\\d{14}$"), "CNPJ")
        .when(col("cnpj_cpf_fornecedor").rlike("^\\d{11}$"), "CPF")
        .otherwise("OUTRO"))
)

# Exibe quantidade de fornecedores unicos
print(f"   Fornecedores unicos: {df_fornecedores.count()}")

# COMMAND ----------

# DBTITLE 1,Validacao e Gravacao
# MAGIC %md
# MAGIC # Validacao de Qualidade e Gravacao

# COMMAND ----------

# DBTITLE 1,Sobre a Validacao
# MAGIC %md
# MAGIC Na celula abaixo e verificada a qualidade do fato de despesas (campos obrigatorios
# MAGIC e duplicatas) e depois ambas as tabelas sao gravadas via MERGE.

# COMMAND ----------

# DBTITLE 1,Valida e Grava
# ============================================================
# VALIDACAO E GRAVACAO NA SILVER
# ============================================================
# Valida qualidade e grava fato_despesas e dim_fornecedores.
# ============================================================

# Valida campos obrigatorios do fato
check_quality(df_silver, "fato_despesas",
    key_columns=["sk_despesa"],
    critical_columns=["id_deputado", "valor_liquido", "categoria_despesa"])

# Grava fato_despesas via MERGE
merge_to_silver(df_silver, "fato_despesas", key_columns=["sk_despesa"])

# Registra status do fato
status_list.append({"tabela": "ft_silver.fato_despesas", "registros": df_silver.count()})

# Grava dim_fornecedores via MERGE
merge_to_silver(df_fornecedores, "dim_fornecedores", key_columns=["cnpj_cpf_fornecedor"])

# Registra status da dimensao
status_list.append({"tabela": "ft_silver.dim_fornecedores", "registros": df_fornecedores.count()})

# COMMAND ----------

# DBTITLE 1,Finalizacao
# MAGIC %md
# MAGIC # Finalizacao do Notebook

# COMMAND ----------

# DBTITLE 1,Sobre a Finalizacao
# MAGIC %md
# MAGIC Na celula abaixo e encerrado o processamento. O sistema calcula o tempo total
# MAGIC e exibe um resumo com todas as tabelas processadas e seus volumes.

# COMMAND ----------

# DBTITLE 1,Finaliza Notebook
# ============================================================
# FINALIZACAO E RESUMO
# ============================================================
# Exibe metricas de tempo e registros processados.
# Registra o fim do notebook no sistema de logs.
# ============================================================

# Chama funcao de finalizacao que exibe resumo e registra log
finalizar_notebook()

