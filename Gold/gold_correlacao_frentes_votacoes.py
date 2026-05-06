# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Gold - Coesao: Frentes vs Partidos
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook analisa a coesao nas votacoes entre membros de frentes parlamentares
# MAGIC e entre membros de partidos. Mede se deputados da mesma frente votam juntos
# MAGIC (coesao de frente) e se deputados do mesmo partido votam alinhados (coesao partidaria).
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `dt0025_dev.ft_silver.fato_votos` | Votos individuais por deputado |
# MAGIC | `dt0025_dev.ft_bronze.frentes_membros` | Membros de cada frente |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_gold.coesao_frentes` | Indice de coesao por frente |
# MAGIC | `dt0025_dev.ft_gold.coesao_partidos` | Indice de coesao por partido |
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
log_notebook_start("gold_correlacao_frentes_votacoes")

# COMMAND ----------

# DBTITLE 1,Sobre: Carrega Dados
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Carrega Dados**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Carrega Dados
# ============================================================
# CARREGA VOTOS E FRENTES
# ============================================================
# Lê fato_votos da Silver e atlas_frentes da Gold para
# cruzar participação em frentes com comportamento de voto.
# ============================================================

print("📖 Carregando dados...")

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_SILVER}.fato_votos"
df_votos = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.fato_votos")
# Carrega dados da tabela f"{CATALOG}.{SCHEMA_GOLD}.atlas_frentes"
df_frentes = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.atlas_frentes")

# Exibe mensagem informativa para o usuario
print(f"   Votos: {df_votos.count()}")
# Exibe mensagem informativa para o usuario
print(f"   Frentes-membros: {df_frentes.count()}")

# COMMAND ----------

# DBTITLE 1,Sobre: Coesão Por Frente
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Coesão Por Frente**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Coesão Por Frente
# ============================================================
# ÍNDICE DE COESÃO POR FRENTE
# ============================================================
# Para cada votação, calcula a fração do voto majoritário
# entre membros de cada frente.
# Coesão = votos_da_maioria / total_votos_da_frente
# Média de coesão por frente em todas as votações.
# Valor 1.0 = unanimidade; 0.5 = divisão total.
# ============================================================

print("📊 Calculando coesão por frente...")

# Junta votos com frentes (cada deputado pode estar em múltiplas frentes)
df_votos_frentes = (df_votos
    # Faz join (cruzamento) com outra tabela
    .join(df_frentes.select("id_deputado", "id_frente", "titulo_frente"),
        # Atribui valor a variavel 'on'
        on="id_deputado")
# Fecha bloco de parametros
)

# Para cada (frente, votação): conta votos por tipo
w_frente_vot = Window.partitionBy("id_frente", "id_votacao")

# Inicia cadeia de transformacoes PySpark
df_coesao_votacao = (df_votos_frentes
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_frente", "titulo_frente", "id_votacao", "tipo_voto_padronizado")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("n_votos"))
# Fecha bloco de parametros
)

# Voto majoritário por frente por votação
w_max = Window.partitionBy("id_frente", "id_votacao").orderBy(col("n_votos").desc())

# Inicia cadeia de transformacoes PySpark
df_maioria = (df_coesao_votacao
    # Adiciona ou modifica a coluna 'rank'
    .withColumn("rank", row_number().over(w_max))
    # Filtra registros conforme condicao especificada
    .filter(col("rank") == 1)
    # Seleciona as colunas desejadas para o resultado
    .select("id_frente", "id_votacao", col("n_votos").alias("votos_maioria"))
# Fecha bloco de parametros
)

# Inicia cadeia de transformacoes PySpark
df_total = (df_coesao_votacao
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_frente", "id_votacao")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(spark_sum("n_votos").alias("total_votos"))
# Fecha bloco de parametros
)

# Coesão por frente (média de todas as votações)
df_coesao_frente = (df_maioria
    # Faz join (cruzamento) com outra tabela
    .join(df_total, on=["id_frente", "id_votacao"])
    # Adiciona ou modifica a coluna 'coesao'
    .withColumn("coesao", col("votos_maioria") / col("total_votos"))
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_frente")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(
        # Renomeia coluna para 'indice_coesao_medio'
        avg("coesao").alias("indice_coesao_medio"),
        # Conta registros
        count("*").alias("n_votacoes_analisadas"),
        # Renomeia coluna para 'coesao_minima'
        spark_min("coesao").alias("coesao_minima"),
        # Renomeia coluna para 'coesao_maxima'
        spark_max("coesao").alias("coesao_maxima")
    # Fecha bloco de parametros
    )
    # Faz join (cruzamento) com outra tabela
    .join(df_frentes.select("id_frente", "titulo_frente").distinct(), on="id_frente")
    # Ordena resultados
    .orderBy(col("indice_coesao_medio").desc())
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_coesao_frente, "coesao_frentes")
# Registra status para o resumo final
status_processamento("ft_gold.coesao_frentes", df_coesao_frente.count())

# Exibe mensagem informativa para o usuario
print("\n   TOP 10 frentes mais coesas:")
# Exibe amostra dos resultados no console
df_coesao_frente.select("titulo_frente", "indice_coesao_medio", "n_votacoes_analisadas").show(10, truncate=40)

# COMMAND ----------

# DBTITLE 1,Sobre: Coesão Por Partido
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Coesão Por Partido**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Coesão Por Partido
# ============================================================
# ÍNDICE DE COESÃO POR PARTIDO
# ============================================================
# Mesma metodologia aplicada aos partidos para comparação.
# Permite verificar: frentes são mais coesas que partidos?
# ============================================================

print("📊 Calculando coesão por partido...")

# Inicia cadeia de transformacoes PySpark
df_coesao_partido_vot = (df_votos
    # Agrupa registros pelas colunas indicadas
    .groupBy("sigla_partido", "id_votacao", "tipo_voto_padronizado")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("n_votos"))
# Fecha bloco de parametros
)

# Define janela de particao para calculos por grupo
w_max_p = Window.partitionBy("sigla_partido", "id_votacao").orderBy(col("n_votos").desc())

# Inicia cadeia de transformacoes PySpark
df_maioria_p = (df_coesao_partido_vot
    # Adiciona ou modifica a coluna 'rank'
    .withColumn("rank", row_number().over(w_max_p))
    # Filtra registros conforme condicao especificada
    .filter(col("rank") == 1)
    # Seleciona as colunas desejadas para o resultado
    .select("sigla_partido", "id_votacao", col("n_votos").alias("votos_maioria"))
# Fecha bloco de parametros
)

# Inicia cadeia de transformacoes PySpark
df_total_p = (df_coesao_partido_vot
    # Agrupa registros pelas colunas indicadas
    .groupBy("sigla_partido", "id_votacao")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(spark_sum("n_votos").alias("total_votos"))
# Fecha bloco de parametros
)

# Inicia cadeia de transformacoes PySpark
df_coesao_partido = (df_maioria_p
    # Faz join (cruzamento) com outra tabela
    .join(df_total_p, on=["sigla_partido", "id_votacao"])
    # Adiciona ou modifica a coluna 'coesao'
    .withColumn("coesao", col("votos_maioria") / col("total_votos"))
    # Agrupa registros pelas colunas indicadas
    .groupBy("sigla_partido")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(
        # Renomeia coluna para 'indice_coesao_medio'
        avg("coesao").alias("indice_coesao_medio"),
        # Conta registros
        count("*").alias("n_votacoes")
    # Fecha bloco de parametros
    )
    # Ordena resultados
    .orderBy(col("indice_coesao_medio").desc())
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_coesao_partido, "coesao_partidos")
# Registra status para o resumo final
status_processamento("ft_gold.coesao_partidos", df_coesao_partido.count())

# COMMAND ----------

# DBTITLE 1,Sobre: Comparativo Frente vs Partido
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Comparativo Frente vs Partido**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Comparativo Frente vs Partido
# ============================================================
# COMPARATIVO: FRENTE MAIS COESA QUE PARTIDO?
# ============================================================
# Compara o índice de coesão médio das frentes com o dos
# partidos para responder: deputados de mesma frente votam
# mais alinhados que colegas de partido?
# ============================================================

print("📊 Comparativo Frente vs Partido...")

# Atribui valor a variavel 'media_frentes'
media_frentes = df_coesao_frente.select(avg("indice_coesao_medio")).first()[0]
# Atribui valor a variavel 'media_partidos'
media_partidos = df_coesao_partido.select(avg("indice_coesao_medio")).first()[0]

# Exibe mensagem informativa para o usuario
print(f"\n   📌 Coesão média das FRENTES: {media_frentes:.4f}")
# Exibe mensagem informativa para o usuario
print(f"   📌 Coesão média dos PARTIDOS: {media_partidos:.4f}")

# Verifica condicao
if media_frentes and media_partidos:
    # Verifica condicao
    if media_frentes > media_partidos:
        # Exibe mensagem informativa para o usuario
        print(f"\n   ✅ CONCLUSÃO: Frentes são MAIS coesas que partidos (+{(media_frentes-media_partidos)*100:.1f}%)")
    # Caso alternativo da condicao
    else:
        # Exibe mensagem informativa para o usuario
        print(f"\n   ℹ️ CONCLUSÃO: Partidos são MAIS coesos que frentes (+{(media_partidos-media_frentes)*100:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Finaliza Notebook
# ============================================================
# FINALIZAÇÃO E RESUMO
# ============================================================
# Exibe métricas de tempo e registros processados.
# ============================================================

finalizar_notebook()
