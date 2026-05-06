# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Bronze - Orgaos e Partidos
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook realiza a ingestao dos dados de referencia (dimensoes) da Camara dos Deputados:
# MAGIC orgaos legislativos (comissoes, CPIs, plenario), partidos politicos e legislaturas.
# MAGIC Estes dados sao relativamente estaveis e mudam com pouca frequencia, por isso a
# MAGIC execucao e semanal. Eles servem como base para as dimensoes na camada Gold.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|-----------|
# MAGIC | `GET /orgaos` | Comissoes permanentes, temporarias, CPIs, plenario, mesa diretora |
# MAGIC | `GET /partidos` | Partidos politicos ativos e extintos |
# MAGIC | `GET /legislaturas` | Legislaturas com data inicio e fim |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|-----------|
# MAGIC | `dt0025_dev.ft_bronze.orgaos` | Orgaos legislativos (id, sigla, nome, tipo) |
# MAGIC | `dt0025_dev.ft_bronze.partidos` | Partidos politicos (id, sigla, nome) |
# MAGIC | `dt0025_dev.ft_bronze.legislaturas` | Legislaturas (id, dataInicio, dataFim) |
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
log_notebook_start("bronze_orgaos_partidos")

# COMMAND ----------

# DBTITLE 1,Ingestao dos Dados
# MAGIC %md
# MAGIC # Ingestao dos Dados de Referencia

# COMMAND ----------

# DBTITLE 1,Sobre os Orgaos Legislativos
# MAGIC %md
# MAGIC Na celula abaixo sao buscados todos os orgaos da Camara dos Deputados.
# MAGIC Um orgao pode ser uma comissao permanente, comissao temporaria, CPI (Comissao
# MAGIC Parlamentar de Inquerito), plenario ou mesa diretora. Cada orgao possui um id unico,
# MAGIC uma sigla (ex: CCJC), um nome completo e um tipo.

# COMMAND ----------

# DBTITLE 1,Ingere Orgaos
# ============================================================
# INGESTAO DOS ORGAOS LEGISLATIVOS
# ============================================================
# Busca todos os orgaos: comissoes permanentes, temporarias,
# CPIs, plenario, mesa diretora. Cada orgao tem id, sigla,
# nome e tipo. Base para dim_orgao na Gold.
# ============================================================

# Informa o usuario que a ingestao esta iniciando
print("Ingerindo orgaos legislativos...")

# Busca todos os orgaos da API (paginacao automatica)
orgaos = fetch_api("/orgaos", params={"itens": 200})

# Exibe quantidade encontrada
print(f"   Orgaos: {len(orgaos)}")

# Grava os dados na tabela bronze
n1 = save_to_bronze(orgaos, "orgaos", "/orgaos")

# Registra status para o resumo final
status_list.append({"tabela": "ft_bronze.orgaos", "registros": n1})

# COMMAND ----------

# DBTITLE 1,Sobre os Partidos Politicos
# MAGIC %md
# MAGIC Na celula abaixo sao buscados todos os partidos politicos registrados na Camara.
# MAGIC A lista inclui tanto partidos ativos quanto extintos, para permitir analises historicas.
# MAGIC Cada partido possui id, sigla (ex: PT, PL, PSDB) e nome completo.

# COMMAND ----------

# DBTITLE 1,Ingere Partidos
# ============================================================
# INGESTAO DOS PARTIDOS POLITICOS
# ============================================================
# Busca todos os partidos registrados com sigla e nome.
# Inclui partidos ativos e extintos para historico.
# ============================================================

# Informa o usuario que a ingestao esta iniciando
print("Ingerindo partidos politicos...")

# Busca todos os partidos da API
partidos = fetch_api("/partidos", params={"itens": 100})

# Exibe quantidade encontrada
print(f"   Partidos: {len(partidos)}")

# Grava os dados na tabela bronze
n2 = save_to_bronze(partidos, "partidos", "/partidos")

# Registra status para o resumo final
status_list.append({"tabela": "ft_bronze.partidos", "registros": n2})

# COMMAND ----------

# DBTITLE 1,Sobre as Legislaturas
# MAGIC %md
# MAGIC Na celula abaixo sao buscadas todas as legislaturas (mandatos de 4 anos).
# MAGIC Cada legislatura possui um numero sequencial, data de inicio e data de fim.
# MAGIC Esses dados sao necessarios para analises temporais e comparacao entre periodos.

# COMMAND ----------

# DBTITLE 1,Ingere Legislaturas
# ============================================================
# INGESTAO DAS LEGISLATURAS
# ============================================================
# Busca todas as legislaturas com data inicio e fim.
# Necessario para analises temporais e evolucao entre
# legislaturas (ex: evolucao de frentes por tema).
# ============================================================

# Informa o usuario que a ingestao esta iniciando
print("Ingerindo legislaturas...")

# Busca todas as legislaturas da API
legislaturas = fetch_api("/legislaturas", params={"itens": 100})

# Exibe quantidade encontrada
print(f"   Legislaturas: {len(legislaturas)}")

# Grava os dados na tabela bronze
n3 = save_to_bronze(legislaturas, "legislaturas", "/legislaturas")

# Registra status para o resumo final
status_list.append({"tabela": "ft_bronze.legislaturas", "registros": n3})

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

