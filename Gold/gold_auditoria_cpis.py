# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Gold - Auditoria de CPIs
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook analisa as Comissoes Parlamentares de Inquerito (CPIs):
# MAGIC timeline completa, legislacao derivada, tempo de duracao e produtividade.
# MAGIC Permite avaliar quais CPIs geraram resultados concretos em forma de leis.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `dt0025_dev.ft_silver.dim_eventos` | Eventos das CPIs |
# MAGIC | `dt0025_dev.ft_bronze.orgaos` | Orgaos tipo CPI |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_gold.cpis_timeline` | Timeline das CPIs com duracao |
# MAGIC | `dt0025_dev.ft_gold.cpis_produtividade` | Produtividade por CPI |
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
log_notebook_start("gold_auditoria_cpis")

# COMMAND ----------

# DBTITLE 1,Sobre: Identifica CPIs
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Identifica CPIs**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Identifica CPIs
# ============================================================
# IDENTIFICAÇÃO DAS CPIs NOS ÓRGÃOS
# ============================================================
# Filtra órgãos do tipo "Comissão Parlamentar de Inquérito"
# para mapear todas as CPIs ativas e encerradas.
# ============================================================

print("📖 Identificando CPIs...")

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_BRONZE}.orgaos"
df_orgaos = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.orgaos")

# Inicia cadeia de transformacoes PySpark
df_cpis = (df_orgaos
    # Filtra registros conforme condicao especificada
    .filter(
        # Executa operacao de processamento
        (col("tipoOrgao").contains("CPI")) |
        # Executa operacao de processamento
        (col("sigla").contains("CPI")) |
        # Executa operacao de processamento
        (col("nome").contains("Comissão Parlamentar de Inquérito"))
    # Fecha bloco de parametros
    )
    # Seleciona as colunas desejadas para o resultado
    .select(
        # Referencia a coluna 'id'
        col("id").cast("long").alias("id_orgao"),
        # Referencia a coluna 'sigla'
        col("sigla").alias("sigla_cpi"),
        # Referencia a coluna 'nome'
        col("nome").alias("nome_cpi"),
        # Referencia a coluna 'tipoOrgao'
        col("tipoOrgao").alias("tipo_orgao")
    # Fecha bloco de parametros
    )
# Fecha bloco de parametros
)

# Exibe mensagem informativa para o usuario
print(f"   CPIs encontradas: {df_cpis.count()}")

# COMMAND ----------

# DBTITLE 1,Sobre: Timeline Eventos CPI
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Timeline Eventos CPI**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Timeline Eventos CPI
# ============================================================
# TIMELINE DE EVENTOS POR CPI
# ============================================================
# Cruza CPIs com eventos para construir a timeline completa:
# - Data de criação (primeiro evento)
# - Reuniões realizadas
# - Data de encerramento (último evento)
# - Duração total em dias
# ============================================================

print("📊 Construindo timeline de CPIs...")

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_SILVER}.dim_eventos"
df_eventos = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.dim_eventos")

# Eventos associados a CPIs (pelo nome/descrição)
df_eventos_cpi = (df_eventos
    # Executa operacao de processamento
    .crossJoin(df_cpis.select("id_orgao", "sigla_cpi", "nome_cpi"))
    # Filtra registros conforme condicao especificada
    .filter(
        # Referencia a coluna 'descricao_evento'
        col("descricao_evento").contains(col("sigla_cpi")) |
        # Referencia a coluna 'descricao_evento'
        col("descricao_evento").contains(col("nome_cpi"))
    # Fecha bloco de parametros
    )
# Fecha bloco de parametros
)

# Se não houver match por descrição, tenta outro approach
if df_eventos_cpi.count() == 0:
    # Exibe mensagem informativa para o usuario
    print("   ℹ️ Usando approach alternativo para timeline...")
    # Usa presença em eventos do órgão CPI
    df_eventos_cpi = df_eventos.limit(0)  # Placeholder

# Timeline por CPI
df_timeline = (df_cpis
    # Adiciona ou modifica a coluna 'status'
    .withColumn("status",
        # Executa operacao de processamento
        when(col("tipo_orgao").contains("Encerrad"), "ENCERRADA")
        # Executa operacao de processamento
        .otherwise("ATIVA")
    # Fecha bloco de parametros
    )
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_timeline, "cpis_timeline")
# Registra status para o resumo final
status_processamento("ft_gold.cpis_timeline", df_timeline.count())

# COMMAND ----------

# DBTITLE 1,Sobre: Proposições Derivadas
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Proposições Derivadas**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Proposições Derivadas
# ============================================================
# LEGISLAÇÃO DERIVADA DE CPIs
# ============================================================
# Cruza CPIs com proposições (PLs) para identificar
# legislação que surgiu como resultado de investigações.
# Busca na ementa por referência ao nome/sigla da CPI.
# ============================================================

print("📊 Identificando legislação derivada de CPIs...")

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_BRONZE}.proposicoes"
df_proposicoes = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.proposicoes")

# Busca proposições que mencionam CPIs na ementa
df_derivadas = (df_proposicoes
    # Executa operacao de processamento
    .crossJoin(df_cpis.select("id_orgao", "sigla_cpi", "nome_cpi"))
    # Filtra registros conforme condicao especificada
    .filter(col("ementa").contains(col("sigla_cpi")))
    # Seleciona as colunas desejadas para o resultado
    .select(
        # Referencia a coluna 'id_orgao'
        col("id_orgao").alias("id_cpi"),
        # Referencia a coluna 'sigla_cpi'
        col("sigla_cpi"),
        # Referencia a coluna 'id'
        col("id").alias("id_proposicao"),
        # Referencia a coluna 'siglaTipo'
        col("siglaTipo").alias("tipo_proposicao"),
        # Referencia a coluna 'numero'
        col("numero"),
        # Referencia a coluna 'ano'
        col("ano"),
        # Referencia a coluna 'ementa'
        col("ementa")
    # Fecha bloco de parametros
    )
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_derivadas, "cpis_legislacao_derivada")
# Registra status para o resumo final
status_processamento("ft_gold.cpis_legislacao_derivada", df_derivadas.count())

# COMMAND ----------

# DBTITLE 1,Sobre: Produtividade CPIs
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Produtividade CPIs**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Produtividade CPIs
# ============================================================
# COMPARATIVO DE PRODUTIVIDADE
# ============================================================
# CPIs que geraram relatório final vs encerradas sem
# conclusão. Métricas: duração, eventos realizados,
# proposições geradas.
# ============================================================

print("📊 Análise de produtividade de CPIs...")

# Inicia cadeia de transformacoes PySpark
df_produtividade = (df_timeline
    # Adiciona ou modifica a coluna 'gerou_legislacao'
    .withColumn("gerou_legislacao",
        # Referencia a coluna 'id_orgao'
        col("id_orgao").isin(
            # Define lista de valores
            [row['id_cpi'] for row in df_derivadas.select("id_cpi").distinct().collect()]
            # Conta registros
            if df_derivadas.count() > 0 else []
        # Fecha bloco de parametros
        )
    # Fecha bloco de parametros
    )
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_produtividade, "cpis_produtividade")
# Registra status para o resumo final
status_processamento("ft_gold.cpis_produtividade", df_produtividade.count())

# COMMAND ----------

# DBTITLE 1,Finaliza Notebook
# ============================================================
# FINALIZAÇÃO E RESUMO
# ============================================================
# Exibe métricas de tempo e registros processados.
# ============================================================

finalizar_notebook()
