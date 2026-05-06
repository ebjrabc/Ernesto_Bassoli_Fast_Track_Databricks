# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Gold - Monitor de Presenca e Engajamento
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook calcula o score de engajamento parlamentar ponderado:
# MAGIC presenca (40%) + votacoes (60%). Identifica padroes de ausencia,
# MAGIC classifica deputados por percentil e gera serie temporal de presenca.
# MAGIC Permite identificar deputados com baixo engajamento.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `dt0025_dev.ft_silver.fato_presenca` | Presenca em eventos |
# MAGIC | `dt0025_dev.ft_silver.fato_votos` | Participacao em votacoes |
# MAGIC | `dt0025_dev.ft_silver.dim_deputados` | Dimensao deputados |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_gold.engajamento_score` | Score ponderado por deputado |
# MAGIC | `dt0025_dev.ft_gold.presenca_serie_temporal` | Serie temporal de presenca |
# MAGIC | `dt0025_dev.ft_gold.presenca_percentil` | Ranking por percentil |
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
log_notebook_start("gold_monitor_presenca")

# COMMAND ----------

# DBTITLE 1,Sobre: Carrega Dados Silver
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Carrega Dados Silver**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Carrega Dados Silver
# ============================================================
# CARREGA TABELAS SILVER NECESSÁRIAS
# ============================================================
# Lê fato_presenca, fato_votos e dim_eventos para cruzar
# presença com participação em votações.
# ============================================================

print("📖 Carregando dados Silver...")

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_SILVER}.fato_presenca"
df_presenca = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.fato_presenca")
# Carrega dados da tabela f"{CATALOG}.{SCHEMA_SILVER}.fato_votos"
df_votos = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.fato_votos")
# Carrega dados da tabela f"{CATALOG}.{SCHEMA_SILVER}.dim_eventos"
df_eventos = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.dim_eventos")
# Carrega dados da tabela f"{CATALOG}.{SCHEMA_SILVER}.dim_deputados"
df_deputados = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.dim_deputados")

# Exibe mensagem informativa para o usuario
print(f"   Presenças: {df_presenca.count()}")
# Exibe mensagem informativa para o usuario
print(f"   Votos: {df_votos.count()}")
# Exibe mensagem informativa para o usuario
print(f"   Eventos: {df_eventos.count()}")

# COMMAND ----------

# DBTITLE 1,Sobre: Taxa Presença Deputado
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Taxa Presença Deputado**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Taxa Presença Deputado
# ============================================================
# TAXA DE PRESENÇA POR DEPUTADO E TIPO EVENTO
# ============================================================
# Calcula taxa de presença = eventos presentes / total eventos.
# Segmenta por tipo de evento (sessão, audiência, seminário).
# Base para o componente "presença" do score composto.
# ============================================================

print("📊 Calculando taxa de presença...")

# Total de eventos por tipo
total_eventos = (df_eventos
    # Agrupa registros pelas colunas indicadas
    .groupBy("tipo_evento")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("total_eventos_tipo"))
# Fecha bloco de parametros
)

# Presenças por deputado e tipo
presencas_dep = (df_presenca
    # Faz join (cruzamento) com outra tabela
    .join(df_eventos.select("id_evento", "tipo_evento"), on="id_evento")
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_deputado", "tipo_evento")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("presencas"))
# Fecha bloco de parametros
)

# Taxa
df_taxa_presenca = (presencas_dep
    # Faz join (cruzamento) com outra tabela
    .join(total_eventos, on="tipo_evento")
    # Adiciona ou modifica a coluna 'taxa_presenca'
    .withColumn("taxa_presenca", col("presencas") / col("total_eventos_tipo"))
# Fecha bloco de parametros
)

# COMMAND ----------

# DBTITLE 1,Sobre: Score Engajamento Composto
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Score Engajamento Composto**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Score Engajamento Composto
# ============================================================
# SCORE DE ENGAJAMENTO COMPOSTO
# ============================================================
# Score = presença × votações × atividade
# Componentes normalizados (0 a 1):
# - Presença: presença em eventos / total eventos
# - Votações: votos emitidos / total votações
# - Atividade: (frentes + requerimentos) normalizado
# Resultado: percentil de cada deputado vs média.
# ============================================================

print("📊 Calculando score de engajamento composto...")

# Componente 1: Presença global
presenca_global = (df_presenca
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_deputado")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("total_presencas"))
# Fecha bloco de parametros
)

# Componente 2: Participação em votações
votos_global = (df_votos
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_deputado")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("total_votos"))
# Fecha bloco de parametros
)

# Total de votações realizadas
total_votacoes = df_votos.select("id_votacao").distinct().count()
# Conta registros
total_eventos_global = df_eventos.count()

# Junta componentes
df_score = (df_deputados
    # Seleciona as colunas desejadas para o resultado
    .select("id_deputado", "nome_parlamentar", "sigla_partido", "sigla_uf")
    # Faz join (cruzamento) com outra tabela
    .join(presenca_global, on="id_deputado", how="left")
    # Faz join (cruzamento) com outra tabela
    .join(votos_global, on="id_deputado", how="left")
    # Atribui valor a variavel '.fillna(0, subset'
    .fillna(0, subset=["total_presencas", "total_votos"])
    # Adiciona ou modifica a coluna 'score_presenca'
    .withColumn("score_presenca", 
        # Define valor literal (constante)
        when(lit(total_eventos_global) > 0,
            # Referencia a coluna 'total_presencas'
            col("total_presencas") / lit(total_eventos_global))
        # Define valor literal (constante)
        .otherwise(lit(0.0)))
    # Adiciona ou modifica a coluna 'score_votacoes'
    .withColumn("score_votacoes",
        # Define valor literal (constante)
        when(lit(total_votacoes) > 0,
            # Referencia a coluna 'total_votos'
            col("total_votos") / lit(total_votacoes))
        # Define valor literal (constante)
        .otherwise(lit(0.0)))
    # Adiciona ou modifica a coluna 'score_engajamento'
    .withColumn("score_engajamento",
        # Executa operacao de processamento
        (col("score_presenca") * 0.4 + col("score_votacoes") * 0.6))
# Fecha bloco de parametros
)

# Percentil
w_all = Window.orderBy("score_engajamento")
# Atribui valor a variavel 'df_score'
df_score = df_score.withColumn("percentil", percent_rank().over(w_all))

# Grava resultado na tabela Gold
save_to_gold(df_score, "score_engajamento")
# Registra status para o resumo final
status_processamento("ft_gold.score_engajamento", df_score.count())

# Exibe mensagem informativa para o usuario
print(f"\n   Score médio: {df_score.select(avg('score_engajamento')).first()[0]:.4f}")
# Exibe mensagem informativa para o usuario
print("\n   TOP 10 mais engajados:")
# Executa operacao de processamento
df_score.orderBy(col("score_engajamento").desc()).select(
    # Executa operacao de processamento
    "nome_parlamentar", "sigla_partido", "score_engajamento", "percentil"
# Exibe amostra dos resultados no console
).show(10)

# COMMAND ----------

# DBTITLE 1,Sobre: Padrão Ausência Votações
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Padrão Ausência Votações**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Padrão Ausência Votações
# ============================================================
# DETECÇÃO DE PADRÃO DE AUSÊNCIA
# ============================================================
# Identifica deputados que faltam sistematicamente em
# votações específicas (por tema/órgão). Cruza ausência
# em votações com presença em eventos do mesmo dia para
# detectar seletividade.
# ============================================================

print("📊 Detectando padrões de ausência...")

# Votações que cada deputado PERDEU
# (estava em evento no mesmo dia mas não votou)
df_votacoes_unicas = (df_votos
    # Seleciona as colunas desejadas para o resultado
    .select("id_votacao", "data_votacao", "sigla_orgao")
    # Executa operacao de processamento
    .distinct()
# Fecha bloco de parametros
)

# Para cada deputado, votações no período que ele não participou
df_ausencias = (df_deputados.select("id_deputado", "nome_parlamentar", "sigla_partido")
    # Executa operacao de processamento
    .crossJoin(df_votacoes_unicas)
    # Faz join (cruzamento) com outra tabela
    .join(
        # Define valor literal (constante)
        df_votos.select("id_deputado", "id_votacao").withColumn("votou", lit(True)),
        # Atribui valor a variavel 'on'
        on=["id_deputado", "id_votacao"],
        # Atribui valor a variavel 'how'
        how="left"
    # Fecha bloco de parametros
    )
    # Filtra registros conforme condicao especificada
    .filter(col("votou").isNull())
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_deputado", "nome_parlamentar", "sigla_partido", "sigla_orgao")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("votacoes_perdidas"))
    # Ordena resultados
    .orderBy(col("votacoes_perdidas").desc())
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_ausencias, "padrao_ausencia_votacoes")
# Registra status para o resumo final
status_processamento("ft_gold.padrao_ausencia_votacoes", df_ausencias.count())

# COMMAND ----------

# DBTITLE 1,Sobre: Série Temporal Engajamento
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Série Temporal Engajamento**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Série Temporal Engajamento
# ============================================================
# SÉRIE TEMPORAL DE ENGAJAMENTO
# ============================================================
# Calcula score de engajamento por mês para detectar
# quedas após eventos críticos. Permite visualizar
# tendência ao longo do tempo para cada deputado.
# ============================================================

print("📊 Gerando série temporal de engajamento...")

# Presenças por mês
df_serie = (df_presenca
    # Faz join (cruzamento) com outra tabela
    .join(df_eventos.select("id_evento", "ano", "mes"), on="id_evento")
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_deputado", "ano", "mes")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("presencas_mes"))
# Fecha bloco de parametros
)

# Votos por mês
df_votos_mes = (df_votos
    # Adiciona ou modifica a coluna 'ano'
    .withColumn("ano", col("data_votacao").cast("string").substr(1, 4).cast("int"))
    # Adiciona ou modifica a coluna 'mes'
    .withColumn("mes", col("data_votacao").cast("string").substr(6, 2).cast("int"))
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_deputado", "ano", "mes")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("votos_mes"))
# Fecha bloco de parametros
)

# Junta
df_serie_final = (df_serie
    # Faz join (cruzamento) com outra tabela
    .join(df_votos_mes, on=["id_deputado", "ano", "mes"], how="outer")
    # Executa operacao de processamento
    .fillna(0)
    # Adiciona ou modifica a coluna 'engajamento_mes'
    .withColumn("engajamento_mes", col("presencas_mes") + col("votos_mes"))
    # Faz join (cruzamento) com outra tabela
    .join(df_deputados.select("id_deputado", "nome_parlamentar", "sigla_partido"), on="id_deputado")
    # Ordena resultados
    .orderBy("id_deputado", "ano", "mes")
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_serie_final, "serie_temporal_engajamento")
# Registra status para o resumo final
status_processamento("ft_gold.serie_temporal_engajamento", df_serie_final.count())

# COMMAND ----------

# DBTITLE 1,Finaliza Notebook
# ============================================================
# FINALIZAÇÃO E RESUMO
# ============================================================
# Exibe métricas de tempo e registros processados.
# ============================================================

finalizar_notebook()
