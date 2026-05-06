# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Consulta e Monitoramento de Logs
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook fornece queries SQL prontas para monitorar a execucao do pipeline.
# MAGIC Permite verificar erros recentes, execucoes por notebook, chamadas a API,
# MAGIC volume processado por dia e historico de erros de conexao.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `dt0025_dev.ft_bronze._pipeline_logs` | Tabela de logs do pipeline |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `Nenhuma` | Apenas consultas de monitoramento (read-only) |
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
log_notebook_start("consulta_logs")

# COMMAND ----------

# Databricks notebook source
# DBTITLE 1,Banner
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="120"/>
# MAGIC 
# MAGIC # Consulta de Logs — Monitoramento do Pipeline
# MAGIC 
# MAGIC **Descrição:** Notebook para consultar, filtrar e monitorar os logs de
# MAGIC execução de todo o pipeline Fast Track. Permite verificar erros,
# MAGIC tempos de execução e status de cada notebook.
# MAGIC 
# MAGIC | Item | Detalhe |
# MAGIC |------|---------|
# MAGIC | **Tabela** | dt0025_dev.ft_bronze._pipeline_logs |
# MAGIC | **Uso** | Monitoramento e debugging |
# MAGIC | **Responsável** | Ernesto Bassoli |

# COMMAND ----------

# DBTITLE 1,Últimos Logs
# ============================================================
# ÚLTIMOS 50 LOGS DO PIPELINE
# ============================================================
# Exibe os logs mais recentes para monitoramento rápido.
# ============================================================

# MAGIC %sql
# MAGIC SELECT timestamp, nivel, notebook, etapa, mensagem, registros_afetados, duracao_segundos
# MAGIC FROM dt0025_dev.ft_bronze._pipeline_logs
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 50

# COMMAND ----------

# DBTITLE 1,Sobre as consultas
# MAGIC %md
# MAGIC Na celula abaixo sao executadas consultas SQL para monitorar o pipeline.
# MAGIC Cada consulta mostra um aspecto diferente: erros, execucoes, volumes ou conexoes.

# COMMAND ----------

# DBTITLE 1,Erros Recentes
# ============================================================
# ERROS E ALERTAS NAS ÚLTIMAS 24H
# ============================================================
# Filtra apenas logs de nível ERROR e CRITICAL para
# identificação rápida de problemas no pipeline.
# ============================================================

# MAGIC %sql
# MAGIC SELECT timestamp, nivel, notebook, etapa, mensagem, erro_tipo, erro_stack
# MAGIC FROM dt0025_dev.ft_bronze._pipeline_logs
# MAGIC WHERE nivel IN ('ERROR', 'CRITICAL', 'WARN')
# MAGIC   AND timestamp >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC ORDER BY timestamp DESC

# COMMAND ----------

# DBTITLE 1,Execuções Por Notebook
# ============================================================
# RESUMO DE EXECUÇÕES POR NOTEBOOK
# ============================================================
# Mostra última execução de cada notebook com status
# e duração. Permite identificar notebooks lentos ou falhos.
# ============================================================

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   notebook,
# MAGIC   MAX(CASE WHEN etapa = 'NOTEBOOK_START' THEN timestamp END) as ultimo_inicio,
# MAGIC   MAX(CASE WHEN etapa = 'NOTEBOOK_END' THEN timestamp END) as ultimo_fim,
# MAGIC   MAX(CASE WHEN etapa LIKE '%END%' THEN duracao_segundos END) as duracao_seg,
# MAGIC   MAX(CASE WHEN etapa LIKE '%END%' THEN status END) as ultimo_status,
# MAGIC   SUM(CASE WHEN nivel = 'ERROR' THEN 1 ELSE 0 END) as total_erros
# MAGIC FROM dt0025_dev.ft_bronze._pipeline_logs
# MAGIC WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY notebook
# MAGIC ORDER BY ultimo_inicio DESC

# COMMAND ----------

# DBTITLE 1,Chamadas API
# ============================================================
# MONITORAMENTO DE CHAMADAS À API
# ============================================================
# Resumo de chamadas à API com taxa de sucesso/erro.
# Permite identificar endpoints problemáticos.
# ============================================================

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   etapa,
# MAGIC   COUNT(*) as total_chamadas,
# MAGIC   SUM(CASE WHEN nivel = 'SUCCESS' OR nivel = 'INFO' THEN 1 ELSE 0 END) as sucesso,
# MAGIC   SUM(CASE WHEN nivel IN ('ERROR', 'WARN') THEN 1 ELSE 0 END) as falhas,
# MAGIC   ROUND(SUM(CASE WHEN nivel IN ('ERROR','WARN') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as taxa_erro_pct
# MAGIC FROM dt0025_dev.ft_bronze._pipeline_logs
# MAGIC WHERE etapa LIKE 'API%'
# MAGIC   AND timestamp >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY etapa
# MAGIC ORDER BY falhas DESC

# COMMAND ----------

# DBTITLE 1,Volume Processado
# ============================================================
# VOLUME DE DADOS PROCESSADOS POR DIA
# ============================================================
# Total de registros processados por dia para monitorar
# tendências e identificar quedas de volume.
# ============================================================

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DATE(timestamp) as dia,
# MAGIC   SUM(registros_afetados) as total_registros,
# MAGIC   COUNT(DISTINCT notebook) as notebooks_executados,
# MAGIC   SUM(CASE WHEN nivel = 'ERROR' THEN 1 ELSE 0 END) as erros_dia
# MAGIC FROM dt0025_dev.ft_bronze._pipeline_logs
# MAGIC WHERE registros_afetados IS NOT NULL
# MAGIC   AND timestamp >= current_timestamp() - INTERVAL 30 DAYS
# MAGIC GROUP BY DATE(timestamp)
# MAGIC ORDER BY dia DESC

# COMMAND ----------

# DBTITLE 1,Erros de Conexão
# ============================================================
# HISTÓRICO DE ERROS DE CONEXÃO COM API
# ============================================================
# Filtra especificamente erros de conexão e timeout
# para identificar instabilidades da API.
# ============================================================

# MAGIC %sql
# MAGIC SELECT timestamp, notebook, mensagem, detalhes, erro_tipo
# MAGIC FROM dt0025_dev.ft_bronze._pipeline_logs
# MAGIC WHERE etapa IN ('API_CONNECTION_FAILED', 'API_TIMEOUT', 'API_TIMEOUT_EXHAUSTED', 'API_CONNECTION_EXHAUSTED')
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 30
