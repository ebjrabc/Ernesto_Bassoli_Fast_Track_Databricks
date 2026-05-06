# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Gold - Raio-X de Gastos (Anomalias Z-Score)
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook identifica anomalias nas despesas parlamentares usando z-score estatistico.
# MAGIC O z-score mede quantos desvios-padrao um valor esta da media de seu grupo (categoria x UF).
# MAGIC Classificacao: >2sigma=MEDIA, >2.5sigma=ALTA, >3sigma=CRITICA.
# MAGIC Tambem gera ranking de fornecedores por volume recebido.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `dt0025_dev.ft_silver.fato_despesas` | Fato despesas com valores e categorias |
# MAGIC | `dt0025_dev.ft_silver.dim_deputados` | Dimensao com partido e UF |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_gold.anomalias_despesas` | Despesas anomalas com z-score e severidade |
# MAGIC | `dt0025_dev.ft_gold.ranking_fornecedores` | Top fornecedores por volume |
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
log_notebook_start("gold_raio_x_gastos")

# COMMAND ----------

# DBTITLE 1,Sobre: Carrega Dados Silver
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Carrega Dados Silver**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Carrega Dados Silver
# ============================================================
# CARREGA FATO DESPESAS E DIMENSÕES
# ============================================================
# Lê tabelas Silver necessárias para análise:
# - fato_despesas: todas as despesas validadas
# - dim_deputados: informações do deputado (UF, partido)
# ============================================================

print("📖 Carregando dados Silver...")

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_SILVER}.fato_despesas"
df_despesas = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.fato_despesas")
# Carrega dados da tabela f"{CATALOG}.{SCHEMA_SILVER}.dim_deputados"
df_deputados = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.dim_deputados")

# Enriquece com dados do deputado
df_gastos = (df_despesas
    # Faz join (cruzamento) com outra tabela
    .join(df_deputados.select("id_deputado", "nome_parlamentar", "sigla_partido", "sigla_uf"),
        # Atribui valor a variavel 'on'
        on="id_deputado", how="left")
# Fecha bloco de parametros
)

# Exibe mensagem informativa para o usuario
print(f"   Despesas: {df_gastos.count()}")

# COMMAND ----------

# DBTITLE 1,Sobre: Score Anomalia Z-Score
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Score Anomalia Z-Score**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Score Anomalia Z-Score
# ============================================================
# DETECÇÃO DE ANOMALIAS — Z-SCORE
# ============================================================
# Calcula z-score por (categoria_despesa × sigla_uf):
# z = (valor - média) / desvio_padrão
#
# Anomalias são definidas como |z| > 2 (fora de 2σ).
# Permite identificar gastos atípicos considerando o
# padrão regional de cada categoria de despesa.
# ============================================================

print("📊 Calculando z-score de anomalias...")

# Estatísticas por categoria × UF
w_cat_uf = Window.partitionBy("categoria_despesa", "sigla_uf")

# Inicia cadeia de transformacoes PySpark
df_anomalias = (df_gastos
    # Adiciona ou modifica a coluna 'media_cat_uf'
    .withColumn("media_cat_uf", avg("valor_liquido").over(w_cat_uf))
    # Adiciona ou modifica a coluna 'stddev_cat_uf'
    .withColumn("stddev_cat_uf", 
        # Executa operacao de processamento
        coalesce(
            # Soma valores
            spark_sum(
                # Executa operacao de processamento
                (col("valor_liquido") - avg("valor_liquido").over(w_cat_uf)) ** 2
            # Conta registros
            ).over(w_cat_uf) / (count("*").over(w_cat_uf) - 1),
            # Define valor literal (constante)
            lit(1.0)
        # Fecha bloco de parametros
        ) ** 0.5
    # Fecha bloco de parametros
    )
    # Adiciona ou modifica a coluna 'z_score'
    .withColumn("z_score", 
        # Executa operacao de processamento
        when(col("stddev_cat_uf") > 0,
            # Executa operacao de processamento
            (col("valor_liquido") - col("media_cat_uf")) / col("stddev_cat_uf")
        # Fecha bloco de parametros
        ).otherwise(lit(0.0))
    # Fecha bloco de parametros
    )
    # Adiciona ou modifica a coluna 'is_anomalia'
    .withColumn("is_anomalia", 
        # Converte tipo da coluna
        when(col("z_score").cast("double") > 2.0, lit(True))
        # Define valor literal (constante)
        .otherwise(lit(False))
    # Fecha bloco de parametros
    )
    # Adiciona ou modifica a coluna 'severidade'
    .withColumn("severidade",
        # Executa operacao de processamento
        when(col("z_score") > 3.0, "CRITICA")
        # Executa operacao de processamento
        .when(col("z_score") > 2.5, "ALTA")
        # Executa operacao de processamento
        .when(col("z_score") > 2.0, "MEDIA")
        # Executa operacao de processamento
        .otherwise("NORMAL")
    # Fecha bloco de parametros
    )
# Fecha bloco de parametros
)

# Filtra apenas anomalias para tabela gold
df_anomalias_gold = (df_anomalias
    # Filtra registros conforme condicao especificada
    .filter(col("is_anomalia") == True)
    # Seleciona as colunas desejadas para o resultado
    .select(
        # Executa operacao de processamento
        "id_deputado", "nome_parlamentar", "sigla_partido", "sigla_uf",
        # Executa operacao de processamento
        "categoria_despesa", "valor_liquido", "data_documento",
        # Executa operacao de processamento
        "nome_fornecedor", "cnpj_cpf_fornecedor",
        # Executa operacao de processamento
        "z_score", "severidade", "media_cat_uf"
    # Fecha bloco de parametros
    )
    # Ordena resultados
    .orderBy(col("z_score").desc())
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_anomalias_gold, "gastos_anomalias")
# Registra status para o resumo final
status_processamento("ft_gold.gastos_anomalias", df_anomalias_gold.count())

# Exibe mensagem informativa para o usuario
print(f"\n   Anomalias detectadas: {df_anomalias_gold.count()}")
# Exibe mensagem informativa para o usuario
print(f"   Por severidade:")
# Exibe amostra dos resultados no console
df_anomalias_gold.groupBy("severidade").count().show()

# COMMAND ----------

# DBTITLE 1,Sobre: Ranking Fornecedores
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Ranking Fornecedores**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Ranking Fornecedores
# ============================================================
# RANKING DE FORNECEDORES MAIS PAGOS
# ============================================================
# Ranking de fornecedores por valor total recebido.
# Inclui flags para CNPJ suspeito:
# - Fornecedor que atende muitos deputados (>20)
# - Valor médio muito acima da mediana da categoria
# - CNPJ formatado como CPF (pessoa física alta)
# ============================================================

print("📊 Gerando ranking de fornecedores...")

# Inicia cadeia de transformacoes PySpark
df_ranking = (df_gastos
    # Agrupa registros pelas colunas indicadas
    .groupBy("cnpj_cpf_fornecedor", "nome_fornecedor")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(
        # Soma valores
        spark_sum("valor_liquido").alias("valor_total"),
        # Conta registros
        count("*").alias("n_documentos"),
        # Renomeia coluna para 'valor_medio'
        avg("valor_liquido").alias("valor_medio"),
        # Renomeia coluna para 'valor_maximo'
        spark_max("valor_liquido").alias("valor_maximo"),
        # Conta registros
        count(col("id_deputado")).alias("n_deputados_atendidos")
    # Fecha bloco de parametros
    )
    # Filtra registros conforme condicao especificada
    .filter(col("cnpj_cpf_fornecedor").isNotNull())
    # Filtra registros conforme condicao especificada
    .filter(col("cnpj_cpf_fornecedor") != "")
    # Adiciona ou modifica a coluna 'flag_muitos_deputados'
    .withColumn("flag_muitos_deputados", col("n_deputados_atendidos") > 20)
    # Adiciona ou modifica a coluna 'flag_valor_alto'
    .withColumn("flag_valor_alto", col("valor_maximo") > 50000)
    # Adiciona ou modifica a coluna 'flag_pf_alto'
    .withColumn("flag_pf_alto", 
        # Executa operacao de processamento
        (col("cnpj_cpf_fornecedor").rlike("^\\d{11}$")) & 
        # Executa operacao de processamento
        (col("valor_total") > 100000)
    # Fecha bloco de parametros
    )
    # Adiciona ou modifica a coluna 'n_flags'
    .withColumn("n_flags", 
        # Referencia a coluna 'flag_muitos_deputados'
        col("flag_muitos_deputados").cast("int") +
        # Referencia a coluna 'flag_valor_alto'
        col("flag_valor_alto").cast("int") +
        # Referencia a coluna 'flag_pf_alto'
        col("flag_pf_alto").cast("int")
    # Fecha bloco de parametros
    )
    # Ordena resultados
    .orderBy(col("valor_total").desc())
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_ranking, "ranking_fornecedores")
# Registra status para o resumo final
status_processamento("ft_gold.ranking_fornecedores", df_ranking.count())

# Exibe mensagem informativa para o usuario
print(f"\n   Fornecedores com flags: {df_ranking.filter(col('n_flags') > 0).count()}")

# COMMAND ----------

# DBTITLE 1,Sobre: Relatório Mensal Partido
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Relatório Mensal Partido**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Relatório Mensal Partido
# ============================================================
# RELATÓRIO MENSAL POR PARTIDO
# ============================================================
# Agrega gastos mensais por partido com:
# - Total gasto, média por deputado, top categorias
# - Ranking dos 10 maiores gastos individuais
# Automatizado para geração recorrente.
# ============================================================

print("📊 Gerando relatório mensal por partido...")

# Inicia cadeia de transformacoes PySpark
df_relatorio = (df_gastos
    # Agrupa registros pelas colunas indicadas
    .groupBy("sigla_partido", "ano", "mes", "categoria_despesa")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(
        # Soma valores
        spark_sum("valor_liquido").alias("valor_total"),
        # Conta registros
        count("*").alias("n_despesas"),
        # Renomeia coluna para 'valor_medio'
        avg("valor_liquido").alias("valor_medio"),
        # Conta registros
        count(col("id_deputado")).alias("n_deputados")
    # Fecha bloco de parametros
    )
    # Ordena resultados
    .orderBy("sigla_partido", "ano", "mes", col("valor_total").desc())
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_relatorio, "relatorio_mensal_partido")
# Registra status para o resumo final
status_processamento("ft_gold.relatorio_mensal_partido", df_relatorio.count())

# Top 10 gastos por partido (último mês disponível)
df_top10 = (df_gastos
    # Ordena resultados
    .orderBy(col("valor_liquido").desc())
    # Seleciona as colunas desejadas para o resultado
    .select("nome_parlamentar", "sigla_partido", "categoria_despesa", 
            # Executa operacao de processamento
            "valor_liquido", "nome_fornecedor", "data_documento")
    # Executa operacao de processamento
    .limit(100)
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_top10, "top_gastos_individuais")

# COMMAND ----------

# DBTITLE 1,Finaliza Notebook
# ============================================================
# FINALIZAÇÃO E RESUMO
# ============================================================
# Exibe métricas de tempo e registros processados.
# ============================================================

finalizar_notebook()
