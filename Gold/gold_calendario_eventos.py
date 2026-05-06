# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Gold - Calendario e Densidade de Eventos
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook analisa a distribuicao temporal dos eventos legislativos:
# MAGIC densidade semanal, comparacao entre periodos eleitorais e nao-eleitorais,
# MAGIC e projecao de eventos futuros baseada em padroes historicos.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `dt0025_dev.ft_silver.dim_eventos` | Eventos com campos de data |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_gold.calendario_densidade_semanal` | Densidade de eventos por semana |
# MAGIC | `dt0025_dev.ft_gold.calendario_comparacao_eleitoral` | Comparacao periodos eleitorais |
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
log_notebook_start("gold_calendario_eventos")

# COMMAND ----------

# DBTITLE 1,Sobre: Calendário Consolidado
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Calendário Consolidado**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Calendário Consolidado
# ============================================================
# TABELA GOLD: CALENDÁRIO ANALÍTICO
# ============================================================
# Consolida eventos com dimensões completas:
# - dim_orgao: órgão responsável pelo evento
# - dim_tipo_evento: classificação do evento
# - dim_data: ano, mês, semana, dia da semana
# - Métricas: total de presentes, duração
# ============================================================

print("📖 Construindo calendário analítico...")

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_SILVER}.dim_eventos"
df_eventos = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.dim_eventos")
# Carrega dados da tabela f"{CATALOG}.{SCHEMA_SILVER}.fato_presenca"
df_presenca = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.fato_presenca")

# Contagem de presentes por evento
df_presentes = (df_presenca
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_evento")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("total_presentes"))
# Fecha bloco de parametros
)

# Calendário com métricas
df_calendario = (df_eventos
    # Faz join (cruzamento) com outra tabela
    .join(df_presentes, on="id_evento", how="left")
    # Atribui valor a variavel '.fillna(0, subset'
    .fillna(0, subset=["total_presentes"])
    # Adiciona ou modifica a coluna 'duracao_horas'
    .withColumn("duracao_horas",
        # Converte tipo da coluna
        (col("data_hora_fim").cast("long") - col("data_hora_inicio").cast("long")) / 3600)
    # Seleciona as colunas desejadas para o resultado
    .select(
        # Executa operacao de processamento
        "id_evento", "data_evento", "data_hora_inicio", "data_hora_fim",
        # Executa operacao de processamento
        "tipo_evento", "situacao", "descricao_evento",
        # Executa operacao de processamento
        "ano", "mes", "semana_ano",
        # Executa operacao de processamento
        "total_presentes", "duracao_horas"
    # Fecha bloco de parametros
    )
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_calendario, "calendario_analitico")
# Registra status para o resumo final
status_processamento("ft_gold.calendario_analitico", df_calendario.count())

# COMMAND ----------

# DBTITLE 1,Sobre: Densidade Semanal
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Densidade Semanal**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Densidade Semanal
# ============================================================
# DENSIDADE DE EVENTOS POR SEMANA
# ============================================================
# Identifica semanas com alta e baixa atividade.
# Detecta semanas sem nenhum evento (recesso, feriados).
# Métricas: eventos/semana, média de presentes/semana.
# ============================================================

print("📊 Calculando densidade semanal...")

# Inicia cadeia de transformacoes PySpark
df_densidade = (df_calendario
    # Agrupa registros pelas colunas indicadas
    .groupBy("ano", "semana_ano")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(
        # Conta registros
        count("*").alias("n_eventos"),
        # Soma valores
        spark_sum("total_presentes").alias("total_presentes_semana"),
        # Renomeia coluna para 'media_presentes_evento'
        avg("total_presentes").alias("media_presentes_evento"),
        # Renomeia coluna para 'tipos_eventos'
        collect_list("tipo_evento").alias("tipos_eventos")
    # Fecha bloco de parametros
    )
    # Adiciona ou modifica a coluna 'semanas_sem_atividade'
    .withColumn("semanas_sem_atividade", 
        # Define valor literal (constante)
        when(col("n_eventos") == 0, lit(True)).otherwise(lit(False)))
    # Ordena resultados
    .orderBy("ano", "semana_ano")
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_densidade, "densidade_semanal_eventos")
# Registra status para o resumo final
status_processamento("ft_gold.densidade_semanal_eventos", df_densidade.count())

# COMMAND ----------

# DBTITLE 1,Sobre: Comparativo Eleitoral
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Comparativo Eleitoral**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Comparativo Eleitoral
# ============================================================
# FREQUÊNCIA ANTES/DEPOIS PERÍODOS ELEITORAIS
# ============================================================
# Compara presença de deputados antes e depois de
# períodos eleitorais (jul-out nos anos eleitorais).
# Hipótese: presença cai durante campanha eleitoral.
# ============================================================

print("📊 Comparativo pré/pós período eleitoral...")

# Marca período eleitoral (jul-out de anos pares)
df_presenca_enriq = (df_presenca
    # Faz join (cruzamento) com outra tabela
    .join(df_eventos.select("id_evento", "ano", "mes"), on="id_evento")
    # Adiciona ou modifica a coluna 'periodo'
    .withColumn("periodo",
        # Atribui valor a variavel 'when((col("ano") % 2'
        when((col("ano") % 2 == 0) & (col("mes").between(7, 10)), "ELEITORAL")
        # Atribui valor a variavel '.when((col("ano") % 2'
        .when((col("ano") % 2 == 0) & (col("mes").between(3, 6)), "PRE_ELEITORAL")
        # Atribui valor a variavel '.when((col("ano") % 2'
        .when((col("ano") % 2 == 0) & (col("mes") > 10), "POS_ELEITORAL")
        # Executa operacao de processamento
        .otherwise("REGULAR")
    # Fecha bloco de parametros
    )
# Fecha bloco de parametros
)

# Inicia cadeia de transformacoes PySpark
df_comparativo = (df_presenca_enriq
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_deputado", "periodo")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("presencas"))
    # Agrupa registros pelas colunas indicadas
    .groupBy("periodo")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(
        # Renomeia coluna para 'media_presencas'
        avg("presencas").alias("media_presencas"),
        # Conta registros
        count("*").alias("n_deputados")
    # Fecha bloco de parametros
    )
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_comparativo, "comparativo_eleitoral")
# Registra status para o resumo final
status_processamento("ft_gold.comparativo_eleitoral", df_comparativo.count())

# Exibe mensagem informativa para o usuario
print("\n   Resultado comparativo:")
# Exibe amostra dos resultados no console
df_comparativo.show()

# COMMAND ----------

# DBTITLE 1,Sobre: Eventos Futuros
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Eventos Futuros**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Eventos Futuros
# ============================================================
# VIEW: CALENDÁRIO DE EVENTOS FUTUROS
# ============================================================
# Filtra eventos com data futura para exposição pública.
# Mostra próximos eventos agendados com tipo e local.
# ============================================================

print("📊 Gerando view de eventos futuros...")

# Inicia cadeia de transformacoes PySpark
df_futuros = (df_calendario
    # Filtra registros conforme condicao especificada
    .filter(col("data_evento") >= current_timestamp().cast("date"))
    # Filtra registros conforme condicao especificada
    .filter(col("situacao") != "Encerrada")
    # Ordena resultados
    .orderBy("data_hora_inicio")
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_futuros, "eventos_futuros_agendados")
# Registra status para o resumo final
status_processamento("ft_gold.eventos_futuros_agendados", df_futuros.count())

# COMMAND ----------

# DBTITLE 1,Finaliza Notebook
# ============================================================
# FINALIZAÇÃO E RESUMO
# ============================================================
# Exibe métricas de tempo e registros processados.
# ============================================================

finalizar_notebook()
