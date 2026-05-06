# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Silver - Eventos, Presenca e Votos (Star Schema)
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook transforma dados brutos de eventos, presencas e votacoes em star schema:
# MAGIC dim_eventos (dimensao), fato_presenca (grain: deputado x evento) e fato_votos (grain: deputado x votacao).
# MAGIC Sao a base para o calculo do score de engajamento e analise de coesao partidaria.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `dt0025_dev.ft_bronze.eventos` | Eventos com data, tipo, situacao |
# MAGIC | `dt0025_dev.ft_bronze.eventos_presenca` | Deputados presentes em cada evento |
# MAGIC | `dt0025_dev.ft_bronze.votos` | Votos individuais por votacao |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_silver.dim_eventos` | Dimensao eventos com campos de data |
# MAGIC | `dt0025_dev.ft_silver.fato_presenca` | Fato presenca (deputado x evento) |
# MAGIC | `dt0025_dev.ft_silver.fato_votos` | Fato votos (deputado x votacao) |
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

# COMMAND ----------

# DBTITLE 1,Registro de Inicio
# ============================================================
# REGISTRO DE INICIO NO LOG
# ============================================================
# Registra o inicio da execucao deste notebook na tabela
# de logs para rastreabilidade completa do pipeline.
# ============================================================

# Registra inicio no sistema de logging centralizado
log_notebook_start("silver_eventos_votacoes")

# COMMAND ----------

# DBTITLE 1,Sobre: Dim Eventos
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Dim Eventos**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Dim Eventos
# ============================================================
# DIMENSÃO EVENTOS
# ============================================================
# Transforma eventos brutos em dimensão limpa com:
# - Tipagem adequada (timestamps, int)
# - Categorização por tipo de evento
# - Extração de campos de data (ano, mês, semana)
# - Deduplicação por id
# ============================================================

print("📖 Construindo dim_eventos...")

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_BRONZE}.eventos"
df_eventos = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.eventos")

# Inicia cadeia de transformacoes PySpark
df_dim_eventos = (df_eventos
    # Seleciona as colunas desejadas para o resultado
    .select(
        # Referencia a coluna 'id'
        col("id").cast("long").alias("id_evento"),
        # Renomeia coluna para 'data_hora_inicio'
        to_timestamp(col("dataHoraInicio")).alias("data_hora_inicio"),
        # Renomeia coluna para 'data_hora_fim'
        to_timestamp(col("dataHoraFim")).alias("data_hora_fim"),
        # Referencia a coluna 'situacao'
        col("situacao"),
        # Referencia a coluna 'descricaoTipo'
        col("descricaoTipo").alias("tipo_evento"),
        # Referencia a coluna 'descricao'
        col("descricao").alias("descricao_evento"),
        # Referencia a coluna 'localExterno'
        col("localExterno").alias("local")
    # Fecha bloco de parametros
    )
    # Filtra registros conforme condicao especificada
    .filter(col("id_evento").isNotNull())
    # Adiciona ou modifica a coluna 'data_evento'
    .withColumn("data_evento", to_date(col("data_hora_inicio")))
    # Adiciona ou modifica a coluna 'ano'
    .withColumn("ano", col("data_hora_inicio").cast("date").cast("string").substr(1, 4).cast("int"))
    # Adiciona ou modifica a coluna 'mes'
    .withColumn("mes", col("data_hora_inicio").cast("date").cast("string").substr(6, 2).cast("int"))
    # Adiciona ou modifica a coluna 'semana_ano'
    .withColumn("semana_ano", 
        # Converte tipo da coluna
        ((datediff(col("data_evento"), lit("2023-01-01")) / 7) + 1).cast("int"))
    # Adiciona ou modifica a coluna 'sk_evento'
    .withColumn("sk_evento", md5(col("id_evento").cast("string")))
    # Remove registros duplicados
    .dropDuplicates(["id_evento"])
# Fecha bloco de parametros
)

# Executa MERGE (upsert) na tabela Silver
merge_to_silver(df_dim_eventos, "dim_eventos", key_columns=["id_evento"])
# Registra status para o resumo final
status_processamento("ft_silver.dim_eventos", df_dim_eventos.count())

# COMMAND ----------

# DBTITLE 1,Sobre: Fato Presença
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Fato Presença**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Fato Presença
# ============================================================
# FATO PRESENÇA (EVENTOS)
# ============================================================
# Relaciona deputados a eventos onde estiveram presentes.
# Grain: 1 registro por (deputado, evento).
# Base para taxa de presença e score de engajamento.
# ============================================================

print("📖 Construindo fato_presenca...")

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_BRONZE}.eventos_presenca"
df_presenca = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.eventos_presenca")

# Inicia cadeia de transformacoes PySpark
df_fato_presenca = (df_presenca
    # Seleciona as colunas desejadas para o resultado
    .select(
        # Referencia a coluna 'id'
        col("id").cast("long").alias("id_deputado"),
        # Referencia a coluna '_evento_id'
        col("_evento_id").cast("long").alias("id_evento"),
        # Referencia a coluna '_evento_data'
        col("_evento_data").alias("data_evento_str"),
        # Referencia a coluna '_evento_tipo'
        col("_evento_tipo").alias("tipo_evento"),
        # Referencia a coluna 'siglaPartido'
        col("siglaPartido").alias("sigla_partido"),
        # Referencia a coluna 'siglaUf'
        col("siglaUf").alias("sigla_uf")
    # Fecha bloco de parametros
    )
    # Filtra registros conforme condicao especificada
    .filter(col("id_deputado").isNotNull())
    # Filtra registros conforme condicao especificada
    .filter(col("id_evento").isNotNull())
    # Adiciona ou modifica a coluna 'data_evento'
    .withColumn("data_evento", to_date(col("data_evento_str")))
    # Adiciona ou modifica a coluna 'sk_presenca'
    .withColumn("sk_presenca", md5(
        # Converte tipo da coluna
        concat_ws("||", col("id_deputado").cast("string"), col("id_evento").cast("string"))
    # Fecha bloco de parametros
    ))
    # Executa operacao de processamento
    .drop("data_evento_str")
    # Remove registros duplicados
    .dropDuplicates(["id_deputado", "id_evento"])
# Fecha bloco de parametros
)

# Executa MERGE (upsert) na tabela Silver
merge_to_silver(df_fato_presenca, "fato_presenca", key_columns=["sk_presenca"])
# Registra status para o resumo final
status_processamento("ft_silver.fato_presenca", df_fato_presenca.count())

# COMMAND ----------

# DBTITLE 1,Sobre: Fato Votos
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Fato Votos**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Fato Votos
# ============================================================
# FATO VOTOS (VOTAÇÕES NOMINAIS)
# ============================================================
# Transforma votos individuais em tabela fato com:
# - Deputado, votação, data, órgão
# - Tipo do voto padronizado (Sim/Não/Abstenção/Obstrução)
# - Grain: 1 registro por (deputado, votação)
# ============================================================

print("📖 Construindo fato_votos...")

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_BRONZE}.votos"
df_votos = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.votos")

# Inicia cadeia de transformacoes PySpark
df_fato_votos = (df_votos
    # Seleciona as colunas desejadas para o resultado
    .select(
        # Referencia a coluna '_votacao_id'
        col("_votacao_id").cast("long").alias("id_votacao"),
        # Referencia a coluna '_votacao_data'
        col("_votacao_data").alias("data_votacao_str"),
        # Referencia a coluna '_sigla_orgao'
        col("_sigla_orgao").alias("sigla_orgao"),
        # Referencia a coluna 'deputado_.id'
        col("deputado_.id").cast("long").alias("id_deputado"),
        # Referencia a coluna 'deputado_.nome'
        col("deputado_.nome").alias("nome_deputado"),
        # Referencia a coluna 'deputado_.siglaPartido'
        col("deputado_.siglaPartido").alias("sigla_partido"),
        # Referencia a coluna 'deputado_.siglaUf'
        col("deputado_.siglaUf").alias("sigla_uf"),
        # Referencia a coluna 'tipoVoto'
        col("tipoVoto").alias("tipo_voto")
    # Fecha bloco de parametros
    )
    # Filtra registros conforme condicao especificada
    .filter(col("id_deputado").isNotNull())
    # Adiciona ou modifica a coluna 'data_votacao'
    .withColumn("data_votacao", to_date(col("data_votacao_str")))
    # Adiciona ou modifica a coluna 'tipo_voto_padronizado'
    .withColumn("tipo_voto_padronizado",
        # Executa operacao de processamento
        when(col("tipo_voto").isin("Sim", "sim"), "SIM")
        # Executa operacao de processamento
        .when(col("tipo_voto").isin("Não", "nao", "não"), "NAO")
        # Executa operacao de processamento
        .when(col("tipo_voto").contains("Abstenção"), "ABSTENCAO")
        # Executa operacao de processamento
        .when(col("tipo_voto").contains("Obstrução"), "OBSTRUCAO")
        # Executa operacao de processamento
        .otherwise(upper(col("tipo_voto")))
    # Fecha bloco de parametros
    )
    # Adiciona ou modifica a coluna 'sk_voto'
    .withColumn("sk_voto", md5(
        # Converte tipo da coluna
        concat_ws("||", col("id_deputado").cast("string"), col("id_votacao").cast("string"))
    # Fecha bloco de parametros
    ))
    # Executa operacao de processamento
    .drop("data_votacao_str")
    # Remove registros duplicados
    .dropDuplicates(["id_deputado", "id_votacao"])
# Fecha bloco de parametros
)

# Executa MERGE (upsert) na tabela Silver
merge_to_silver(df_fato_votos, "fato_votos", key_columns=["sk_voto"])
# Registra status para o resumo final
status_processamento("ft_silver.fato_votos", df_fato_votos.count())

# COMMAND ----------

# DBTITLE 1,Finaliza Notebook
# ============================================================
# FINALIZAÇÃO E RESUMO
# ============================================================
# Exibe métricas de tempo e registros processados.
# ============================================================

finalizar_notebook()
