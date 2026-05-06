# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Silver - Proposicoes SCD Type 2 (Historico de Tramitacao)
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook implementa SCD Type 2 (Slowly Changing Dimension Type 2) para proposicoes.
# MAGIC Diferente do SCD Type 1, aqui e mantido o historico completo de cada mudanca de status.
# MAGIC Cada registro possui valid_from, valid_to e is_current para rastrear toda a evolucao.
# MAGIC Tambem calcula o tempo medio de tramitacao por tipo de proposicao.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `dt0025_dev.ft_bronze.tramitacoes` | Tramitacoes com _payload_hash para CDC |
# MAGIC | `dt0025_dev.ft_bronze.proposicoes` | Proposicoes com tipo, numero, ano, ementa |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_silver.proposicoes_scd2` | Historico SCD2 com valid_from/to/is_current |
# MAGIC | `dt0025_dev.ft_silver.tempo_tramitacao` | Tempo medio de tramitacao por tipo |
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
log_notebook_start("silver_proposicoes_scd2")

# COMMAND ----------

# DBTITLE 1,Sobre: Lê Bronze Tramitações
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Lê Bronze Tramitações**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Lê Bronze Tramitações
# ============================================================
# LEITURA DAS TRAMITAÇÕES BRONZE
# ============================================================
# Carrega o histórico completo de tramitações para
# processamento SCD Type 2. Cada registro representa
# uma mudança de status na proposição.
# ============================================================

print("📖 Lendo bronze.tramitacoes...")

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_BRONZE}.tramitacoes"
df_tram = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.tramitacoes")
# Carrega dados da tabela f"{CATALOG}.{SCHEMA_BRONZE}.proposicoes"
df_prop = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.proposicoes")

# Exibe mensagem informativa para o usuario
print(f"   Tramitações: {df_tram.count()}")
# Exibe mensagem informativa para o usuario
print(f"   Proposições: {df_prop.count()}")

# COMMAND ----------

# DBTITLE 1,Sobre: Transformação SCD Type 2
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Transformação SCD Type 2**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Transformação SCD Type 2
# ============================================================
# CONSTRUÇÃO DO SCD TYPE 2
# ============================================================
# Para cada proposição, ordena tramitações por sequência
# e constrói os campos SCD:
# - valid_from: data/hora do início deste status
# - valid_to: data/hora do próximo status (null se atual)
# - is_current: flag indicando status vigente
# Permite reconstruir o estado de qualquer PL em qualquer
# data usando WHERE valid_from <= @data AND
# (valid_to > @data OR is_current = true)
# ============================================================

print("🔄 Construindo SCD Type 2...")

# Janela para calcular valid_to (próxima tramitação)
w = Window.partitionBy("_proposicao_id").orderBy("sequencia")

# Inicia cadeia de transformacoes PySpark
df_scd = (df_tram
    # Seleciona as colunas desejadas para o resultado
    .select(
        # Referencia a coluna '_proposicao_id'
        col("_proposicao_id").cast("long").alias("id_proposicao"),
        # Referencia a coluna '_sigla_tipo'
        col("_sigla_tipo").alias("sigla_tipo"),
        # Referencia a coluna '_numero'
        col("_numero").alias("numero"),
        # Referencia a coluna '_ano'
        col("_ano").alias("ano"),
        # Referencia a coluna 'sequencia'
        col("sequencia").cast("int"),
        # Renomeia coluna para 'data_hora_tramitacao'
        to_timestamp(col("dataHora")).alias("data_hora_tramitacao"),
        # Referencia a coluna 'siglaOrgao'
        col("siglaOrgao").alias("sigla_orgao"),
        # Referencia a coluna 'regime'
        col("regime"),
        # Referencia a coluna 'descricaoTramitacao'
        col("descricaoTramitacao").alias("descricao_tramitacao"),
        # Referencia a coluna 'descricaoSituacao'
        col("descricaoSituacao").alias("situacao"),
        # Referencia a coluna 'codSituacao'
        col("codSituacao").cast("int").alias("cod_situacao"),
        # Referencia a coluna 'despacho'
        col("despacho"),
        # Referencia a coluna 'ambito'
        col("ambito"),
        # Referencia a coluna 'apreciacao'
        col("apreciacao"),
        # Referencia a coluna '_payload_hash'
        col("_payload_hash").alias("payload_hash")
    # Fecha bloco de parametros
    )
    # Filtra registros conforme condicao especificada
    .filter(col("id_proposicao").isNotNull())
    # Adiciona ou modifica a coluna 'valid_from'
    .withColumn("valid_from", col("data_hora_tramitacao"))
    # Adiciona ou modifica a coluna 'valid_to'
    .withColumn("valid_to", lead("data_hora_tramitacao").over(w))
    # Adiciona ou modifica a coluna 'is_current'
    .withColumn("is_current", col("valid_to").isNull())
    # Adiciona ou modifica a coluna 'sk_tramitacao'
    .withColumn("sk_tramitacao", md5(
        # Executa operacao de processamento
        concat_ws("||", 
            # Referencia a coluna 'id_proposicao'
            col("id_proposicao").cast("string"),
            # Referencia a coluna 'sequencia'
            col("sequencia").cast("string")
        # Fecha bloco de parametros
        )
    # Fecha bloco de parametros
    ))
# Fecha bloco de parametros
)

# Exibe mensagem informativa para o usuario
print(f"   Registros SCD: {df_scd.count()}")
# Exibe mensagem informativa para o usuario
print(f"   Proposições únicas: {df_scd.select('id_proposicao').distinct().count()}")
# Exibe mensagem informativa para o usuario
print(f"   Registros atuais (is_current=true): {df_scd.filter(col('is_current')).count()}")

# COMMAND ----------

# DBTITLE 1,Sobre: Detecção Mudança Plenário
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Detecção Mudança Plenário**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Detecção Mudança Plenário
# ============================================================
# ALERTAS: PL AVANÇA PARA PLENÁRIO OU ARQUIVADO
# ============================================================
# Identifica proposições que mudaram para situações
# relevantes: envio ao Plenário ou arquivamento.
# Registra estes eventos para alertas automáticos.
# ============================================================

print("🔔 Detectando mudanças relevantes...")

# PLs que avançaram para plenário
df_plenario = (df_scd
    # Filtra registros conforme condicao especificada
    .filter(col("is_current") == True)
    # Filtra registros conforme condicao especificada
    .filter(
        # Atribui valor a variavel '(col("sigla_orgao")'
        (col("sigla_orgao") == "PLEN") | 
        # Executa operacao de processamento
        (col("situacao").contains("Plenário"))
    # Fecha bloco de parametros
    )
# Fecha bloco de parametros
)

# PLs arquivados
df_arquivados = (df_scd
    # Filtra registros conforme condicao especificada
    .filter(col("is_current") == True)
    # Filtra registros conforme condicao especificada
    .filter(col("situacao").contains("Arquivad"))
# Fecha bloco de parametros
)

# Exibe mensagem informativa para o usuario
print(f"   PLs em Plenário: {df_plenario.count()}")
# Exibe mensagem informativa para o usuario
print(f"   PLs Arquivados: {df_arquivados.count()}")

# COMMAND ----------

# DBTITLE 1,Sobre: Grava Silver SCD2
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Grava Silver SCD2**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Grava Silver SCD2
# ============================================================
# GRAVAÇÃO COM SCD TYPE 2
# ============================================================
# Grava a tabela SCD usando merge_to_silver com scd_type2=True.
# Chave: id_proposicao + sequencia. Registros anteriores
# têm valid_to preenchido e is_current = false.
# ============================================================

merge_to_silver(df_scd, "proposicoes_scd2", 
    # Atribui valor a variavel 'key_columns'
    key_columns=["id_proposicao", "sequencia"],
    # Atribui valor a variavel 'scd_type2'
    scd_type2=False)  # SCD já construído manualmente
# Registra status para o resumo final
status_processamento("ft_silver.proposicoes_scd2", df_scd.count())

# COMMAND ----------

# DBTITLE 1,Sobre: Exemplo Time Travel
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Exemplo Time Travel**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Exemplo Time Travel
# ============================================================
# DEMONSTRAÇÃO: RECONSTRUÇÃO VIA TIME TRAVEL
# ============================================================
# Exemplo de como reconstruir o estado de um PL em
# qualquer data usando os campos SCD:
# SELECT * FROM proposicoes_scd2
# WHERE id_proposicao = X
#   AND valid_from <= '2024-06-15'
#   AND (valid_to > '2024-06-15' OR is_current = true)
# ============================================================

print("📋 Exemplo de consulta Time Travel:")
# Exibe mensagem informativa para o usuario
print(f"""
    # Executa operacao de processamento
    SELECT id_proposicao, sigla_tipo, numero, ano, situacao, sigla_orgao
    # Executa operacao de processamento
    FROM {CATALOG}.{SCHEMA_SILVER}.proposicoes_scd2
    # Atribui valor a variavel 'WHERE id_proposicao'
    WHERE id_proposicao = <ID>
      # Atribui valor a variavel 'AND valid_from <'
      AND valid_from <= '2024-06-15T00:00:00'
      # Atribui valor a variavel 'AND (valid_to > '2024-06-15T00:00:00' OR is_current'
      AND (valid_to > '2024-06-15T00:00:00' OR is_current = true)
# Executa operacao de processamento
""")

# COMMAND ----------

# DBTITLE 1,Sobre: Tempo Médio Tramitação
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Tempo Médio Tramitação**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Tempo Médio Tramitação
# ============================================================
# TEMPO MÉDIO DE TRAMITAÇÃO
# ============================================================
# Calcula o tempo médio de tramitação (dias entre primeira
# e última tramitação) segmentado por:
# - Tipo de proposição (PL, PEC, MP, PLP, PDL)
# - Partido do autor (via join com proposições)
# - Comissão relatora (sigla_orgao do último despacho)
# Entregável exigido no desafio opcional CDC.
# ============================================================

print("📊 Calculando tempo médio de tramitação...")

# Primeira e última tramitação por proposição
from pyspark.sql.functions import datediff, to_date, min as spark_min, max as spark_max, avg

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_SILVER}.proposicoes_scd2"
df_scd_table = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.proposicoes_scd2")

# Inicia cadeia de transformacoes PySpark
df_duracao = (df_scd_table
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_proposicao", "sigla_tipo")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(
        # Renomeia coluna para 'primeira_tramitacao'
        spark_min("valid_from").alias("primeira_tramitacao"),
        # Renomeia coluna para 'ultima_tramitacao'
        spark_max("valid_from").alias("ultima_tramitacao"),
        # Conta registros
        count("*").alias("total_tramitacoes"),
        # Última comissão (orgão da tramitação mais recente)
        spark_max(when(col("is_current") == True, col("sigla_orgao"))).alias("comissao_relatora")
    # Fecha bloco de parametros
    )
    # Adiciona ou modifica a coluna 'duracao_dias'
    .withColumn("duracao_dias", 
        # Converte tipo da coluna
        datediff(col("ultima_tramitacao").cast("date"), col("primeira_tramitacao").cast("date"))
    # Fecha bloco de parametros
    )
    # Filtra registros conforme condicao especificada
    .filter(col("duracao_dias").isNotNull())
    # Filtra registros conforme condicao especificada
    .filter(col("duracao_dias") > 0)
# Fecha bloco de parametros
)

# Tempo médio por TIPO de proposição
df_tempo_tipo = (df_duracao
    # Agrupa registros pelas colunas indicadas
    .groupBy("sigla_tipo")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(
        # Renomeia coluna para 'tempo_medio_dias'
        avg("duracao_dias").alias("tempo_medio_dias"),
        # Conta registros
        count("*").alias("n_proposicoes"),
        # Renomeia coluna para 'min_dias'
        spark_min("duracao_dias").alias("min_dias"),
        # Renomeia coluna para 'max_dias'
        spark_max("duracao_dias").alias("max_dias")
    # Fecha bloco de parametros
    )
    # Ordena resultados
    .orderBy("sigla_tipo")
# Fecha bloco de parametros
)

# Exibe mensagem informativa para o usuario
print("\n   Tempo médio por TIPO:")
# Exibe amostra dos resultados no console
df_tempo_tipo.show()

# Tempo médio por COMISSÃO RELATORA
df_tempo_comissao = (df_duracao
    # Filtra registros conforme condicao especificada
    .filter(col("comissao_relatora").isNotNull())
    # Agrupa registros pelas colunas indicadas
    .groupBy("comissao_relatora")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(
        # Renomeia coluna para 'tempo_medio_dias'
        avg("duracao_dias").alias("tempo_medio_dias"),
        # Conta registros
        count("*").alias("n_proposicoes")
    # Fecha bloco de parametros
    )
    # Filtra registros conforme condicao especificada
    .filter(col("n_proposicoes") >= 5)
    # Ordena resultados
    .orderBy(col("tempo_medio_dias").desc())
# Fecha bloco de parametros
)

# Exibe mensagem informativa para o usuario
print("\n   Tempo médio por COMISSÃO RELATORA (top 10):")
# Exibe amostra dos resultados no console
df_tempo_comissao.show(10)

# Grava tabela gold
from pyspark.sql.functions import current_timestamp

# Atribui valor a variavel 'df_duracao_gold'
df_duracao_gold = df_duracao.withColumn("_processed_at", current_timestamp())
# Atribui valor a variavel 'full_table'
full_table = f"{CATALOG}.{SCHEMA_GOLD}.tempo_medio_tramitacao"
# Executa operacao de processamento
df_duracao_gold.write.format("delta").mode("overwrite").saveAsTable(full_table)
# Registra status para o resumo final
status_processamento("ft_gold.tempo_medio_tramitacao", df_duracao_gold.count())
# Exibe mensagem informativa para o usuario
print(f"  ✅ {full_table} gravada")

# COMMAND ----------

# DBTITLE 1,Finaliza Notebook
# ============================================================
# FINALIZAÇÃO E RESUMO
# ============================================================
# Exibe métricas de tempo e registros processados.
# ============================================================

finalizar_notebook()
