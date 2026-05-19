# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://gazetadasemana.com.br/images/noticias/166864/19041851_compass.uo.jpg.jpg" width="450"/>

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

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Definicao de Funcoes e Parametros
# MAGIC %md
# MAGIC # Definicao de Funcoes, Parametros e Variaveis

# COMMAND ----------

# DBTITLE 1,Sobre o comando run
# MAGIC %md
# MAGIC ## Ordem de Importacao
# MAGIC
# MAGIC **IMPORTANTE:** A ordem de execucao das celulas abaixo e critica:
# MAGIC
# MAGIC 1. **Primeiro**: `%run ../Logs/logger`
# MAGIC    - Carrega as funcoes de logging (log_info, log_error, etc.)
# MAGIC    - Se falhar, o notebook pode continuar (FUNCOES_GENERICAS tem fallback)
# MAGIC
# MAGIC 2. **Depois**: `%run ../FUNCOES_GENERICAS`
# MAGIC    - Carrega todas as funcoes de ingestao, gravacao e validacao
# MAGIC    - Usa as funcoes de logging se estiverem disponiveis
# MAGIC    - Se logging nao foi carregado, usa funcoes dummy (sem quebrar)
# MAGIC
# MAGIC Essa ordem garante que:
# MAGIC - ✅ Logger e carregado primeiro (se disponivel)
# MAGIC - ✅ FUNCOES_GENERICAS pode usar o logger
# MAGIC - ✅ Se logger falhar, FUNCOES_GENERICAS continua funcionando

# COMMAND ----------

# DBTITLE 1,Importacao do Logger
# MAGIC %run ../Logs/logger

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
# INGESTAO DOS ORGAOS LEGISLATIVOS (OTIMIZADO COM CHECKPOINTS)
# ============================================================
# Busca todos os orgaos: comissoes permanentes, temporarias,
# CPIs, plenario, mesa diretora. Cada orgao tem id, sigla,
# nome e tipo. Base para dim_orgao na Gold.
# OTIMIZADO: Grava em lotes de 10 para liberar memoria.
# ============================================================

# Informa o usuario que a ingestao esta iniciando
print("Ingerindo orgaos legislativos (versao otimizada com checkpoints)...")

# Busca todos os orgaos da API (paginacao automatica)
orgaos = fetch_api("/orgaos", params={"itens": 200})

# Exibe quantidade encontrada
print(f"   Orgaos encontrados: {len(orgaos)}")

# Configuracao de checkpoint
BATCH_SIZE = 10  # Grava de 10 em 10
total_gravado = 0

if len(orgaos) == 0:
    print("   Nenhum orgao para processar")
    n1 = 0
else:
    # Processa em lotes de 10
    for i in range(0, len(orgaos), BATCH_SIZE):
        batch = orgaos[i:i+BATCH_SIZE]
        batch_num = i//BATCH_SIZE + 1
        total_batches = (len(orgaos)-1)//BATCH_SIZE + 1
        
        # Define modo: overwrite no primeiro batch, append nos demais
        mode = "overwrite" if i == 0 else "append"
        
        # Grava o batch
        n = save_to_bronze(batch, "orgaos", "/orgaos", mode=mode)
        total_gravado += len(batch)
        
        print(f"   Batch {batch_num}/{total_batches}: Gravados {len(batch)} orgaos (Total: {total_gravado})")
        
        # Limpa memoria do batch
        batch = []
        
        # Pequena pausa entre batches
        if i + BATCH_SIZE < len(orgaos):
            time.sleep(0.3)
    
    n1 = total_gravado
    print(f"   CONCLUIDO: {total_gravado} orgaos gravados no total")

# Limpa memoria
orgaos = []

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
# INGESTAO DOS PARTIDOS POLITICOS (OTIMIZADO COM CHECKPOINTS)
# ============================================================
# Busca todos os partidos registrados com sigla e nome.
# Inclui partidos ativos e extintos para historico.
# OTIMIZADO: Grava em lotes de 10 para liberar memoria.
# ============================================================

# Informa o usuario que a ingestao esta iniciando
print("Ingerindo partidos politicos (versao otimizada com checkpoints)...")

# Busca todos os partidos da API
partidos = fetch_api("/partidos", params={"itens": 100})

# Exibe quantidade encontrada
print(f"   Partidos encontrados: {len(partidos)}")

# Configuracao de checkpoint
BATCH_SIZE = 10  # Grava de 10 em 10
total_gravado = 0

if len(partidos) == 0:
    print("   Nenhum partido para processar")
    n2 = 0
else:
    # Processa em lotes de 10
    for i in range(0, len(partidos), BATCH_SIZE):
        batch = partidos[i:i+BATCH_SIZE]
        batch_num = i//BATCH_SIZE + 1
        total_batches = (len(partidos)-1)//BATCH_SIZE + 1
        
        # Define modo: overwrite no primeiro batch, append nos demais
        mode = "overwrite" if i == 0 else "append"
        
        # Grava o batch
        n = save_to_bronze(batch, "partidos", "/partidos", mode=mode)
        total_gravado += len(batch)
        
        print(f"   Batch {batch_num}/{total_batches}: Gravados {len(batch)} partidos (Total: {total_gravado})")
        
        # Limpa memoria do batch
        batch = []
        
        # Pequena pausa entre batches
        if i + BATCH_SIZE < len(partidos):
            time.sleep(0.3)
    
    n2 = total_gravado
    print(f"   CONCLUIDO: {total_gravado} partidos gravados no total")

# Limpa memoria
partidos = []

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
# INGESTAO DAS LEGISLATURAS (OTIMIZADO COM CHECKPOINTS)
# ============================================================
# Busca todas as legislaturas com data inicio e fim.
# Necessario para analises temporais e evolucao entre
# legislaturas (ex: evolucao de frentes por tema).
# OTIMIZADO: Grava em lotes de 10 para liberar memoria.
# ============================================================

# Informa o usuario que a ingestao esta iniciando
print("Ingerindo legislaturas (versao otimizada com checkpoints)...")

# Busca todas as legislaturas da API
legislaturas = fetch_api("/legislaturas", params={"itens": 100})

# Exibe quantidade encontrada
print(f"   Legislaturas encontradas: {len(legislaturas)}")

# Configuracao de checkpoint
BATCH_SIZE = 10  # Grava de 10 em 10
total_gravado = 0

if len(legislaturas) == 0:
    print("   Nenhuma legislatura para processar")
    n3 = 0
else:
    # Processa em lotes de 10
    for i in range(0, len(legislaturas), BATCH_SIZE):
        batch = legislaturas[i:i+BATCH_SIZE]
        batch_num = i//BATCH_SIZE + 1
        total_batches = (len(legislaturas)-1)//BATCH_SIZE + 1
        
        # Define modo: overwrite no primeiro batch, append nos demais
        mode = "overwrite" if i == 0 else "append"
        
        # Grava o batch
        n = save_to_bronze(batch, "legislaturas", "/legislaturas", mode=mode)
        total_gravado += len(batch)
        
        print(f"   Batch {batch_num}/{total_batches}: Gravadas {len(batch)} legislaturas (Total: {total_gravado})")
        
        # Limpa memoria do batch
        batch = []
        
        # Pequena pausa entre batches
        if i + BATCH_SIZE < len(legislaturas):
            time.sleep(0.3)
    
    n3 = total_gravado
    print(f"   CONCLUIDO: {total_gravado} legislaturas gravadas no total")

# Limpa memoria
legislaturas = []

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
