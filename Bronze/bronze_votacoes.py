# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://gazetadasemana.com.br/images/noticias/166864/19041851_compass.uo.jpg.jpg" width="450"/>

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Bronze - Votacoes Nominais
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook realiza a ingestao incremental das votacoes nominais e votos individuais.
# MAGIC O controle incremental e feito por offset de ID (sequencial), buscando apenas votacoes
# MAGIC com ID maior que o ultimo processado. Para cada votacao, sao extraidos os votos
# MAGIC individuais de cada deputado (Sim/Nao/Abstencao/Obstrucao).
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `GET /votacoes` | Votacoes nominais com id, data, orgao, resultado |
# MAGIC | `GET /votacoes/{id}/votos` | Votos individuais de cada deputado |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_bronze.votacoes` | Votacoes nominais (incremental append) |
# MAGIC | `dt0025_dev.ft_bronze.votos` | Votos individuais por deputado |
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
log_notebook_start("bronze_votacoes")

# COMMAND ----------

# DBTITLE 1,Controle Incremental
# MAGIC %md
# MAGIC # Controle de Carga Incremental por Offset (ID)

# COMMAND ----------

# DBTITLE 1,Sobre o Controle por ID
# MAGIC %md
# MAGIC Na celula abaixo e recuperado o ultimo ID de votacao ja processado.
# MAGIC Diferente do watermark por data, aqui o controle e feito pelo ID da votacao
# MAGIC (que e sequencial). Apenas votacoes com ID maior que o ultimo processado serao buscadas.
# MAGIC Se for a primeira execucao, busca todas desde o inicio da legislatura 57.

# COMMAND ----------

# DBTITLE 1,Verifica Offset ID
# ============================================================
# CONTROLE DE OFFSET POR ID
# ============================================================
# Recupera o ultimo ID de votacao processado para buscar
# apenas votacoes mais recentes. Garante idempotencia e
# eficiencia na carga incremental.
# ============================================================

# Recupera o watermark do controle incremental
watermark = get_watermark("votacoes")

# Parametros da busca de votacoes (ordenado por ID descendente)
params_votacoes = {"ordenarPor": "id", "ordem": "DESC"}

# Verifica se ja existe um watermark com ID
if isinstance(watermark, dict) and watermark.get("last_id"):
    # Carga incremental: busca a partir do ultimo ID
    print(f"   Carga incremental - ultimo ID: {watermark['last_id']}")
else:
    # Primeira execucao: busca desde inicio da legislatura
    params_votacoes["dataInicio"] = "2023-02-01"
    print(f"   Carga completa desde 2023-02-01")

# COMMAND ----------

# DBTITLE 1,Ingestao dos Dados
# MAGIC %md
# MAGIC # Ingestao dos Dados da API

# COMMAND ----------

# DBTITLE 1,Sobre as Votacoes Nominais
# MAGIC %md
# MAGIC Na celula abaixo sao buscadas as votacoes nominais do plenario e comissoes.
# MAGIC Uma votacao nominal e quando cada deputado registra individualmente seu voto
# MAGIC (Sim, Nao, Abstencao ou Obstrucao). Cada votacao possui id, data, orgao responsavel,
# MAGIC proposicao relacionada, descricao e resultado.

# COMMAND ----------

# DBTITLE 1,Ingere Votacoes
# ============================================================
# INGESTAO DAS VOTACOES NOMINAIS (OTIMIZADO COM CHECKPOINTS)
# ============================================================
# Busca votacoes do plenario e comissoes em lotes de 10.
# Grava incrementalmente para liberar memoria.
# ============================================================

print("Ingerindo votacoes (versão otimizada com checkpoints)...")

# Busca votacoes da API com paginacao automatica
votacoes_completas = fetch_api(
    endpoint="/votacoes",
    params=params_votacoes
)

# Filtra apenas votacoes novas (ID maior que o watermark)
if isinstance(watermark, dict) and watermark.get("last_id"):
    votacoes_completas = [v for v in votacoes_completas if str(v['id']) > str(watermark['last_id'])]

print(f"   Votacoes novas encontradas: {len(votacoes_completas)}")

# Configuracoes de checkpoint
BATCH_SIZE = 10  # Processa de 10 em 10
total_gravado = 0

if len(votacoes_completas) == 0:
    print("   Nenhuma votacao nova para processar")
    votacoes_lista = []  # Lista vazia para uso posterior
else:
    # Processa em lotes de 10
    for i in range(0, len(votacoes_completas), BATCH_SIZE):
        batch = votacoes_completas[i:i+BATCH_SIZE]
        batch_num = i//BATCH_SIZE + 1
        total_batches = (len(votacoes_completas)-1)//BATCH_SIZE + 1
        
        print(f"   Batch {batch_num}/{total_batches}: Processando {len(batch)} votacoes...")
        
        # Define modo: overwrite no primeiro batch, append nos demais
        mode = "overwrite" if i == 0 else "append"
        
        # Grava o batch
        n = save_to_bronze(batch, "votacoes", "/votacoes", mode=mode)
        total_gravado += len(batch)
        
        print(f"      Gravadas: {len(batch)} votacoes (Total: {total_gravado})")
        
        # Limpa memoria - mantem apenas IDs para processamento de votos
        if i == 0:
            votacoes_lista = [{'id': v['id'], 'data': v.get('data', v.get('dataHoraRegistro', '')), 'siglaOrgao': v.get('siglaOrgao', '')} for v in votacoes_completas]
        
        # Pausa entre batches
        if i + BATCH_SIZE < len(votacoes_completas):
            time.sleep(0.5)
    
    print(f"   CONCLUIDO: {total_gravado} votacoes gravadas no total")

# COMMAND ----------

# DBTITLE 1,Sobre os Votos Individuais
# MAGIC %md
# MAGIC Na celula abaixo, para cada votacao, sao buscados os votos individuais de cada deputado.
# MAGIC Cada voto contem: deputado, partido, tipo do voto (Sim/Nao/Abstencao/Obstrucao).
# MAGIC Esses dados sao essenciais para analise de coesao partidaria e correlacao com frentes.

# COMMAND ----------

# DBTITLE 1,Ingere Votos Individuais
# ============================================================
# INGESTAO DOS VOTOS DE CADA DEPUTADO (OTIMIZADO)
# ============================================================
# Para cada votacao, busca os votos individuais em lotes de 10.
# Grava incrementalmente para liberar memoria.
# ============================================================

print("Ingerindo votos individuais (modo otimizado com checkpoints)...")

# Lista para acumular votos antes do checkpoint
votos_lista = []

# Contador de registros gravados
total_gravado = 0

# Configuracao de processamento
BATCH_SIZE = 10  # Processa 10 votacoes por vez
MAX_WORKERS = 10  # Threads paralelas

# Total de votacoes para calculo de progresso
total_votacoes = len(votacoes_lista)

if total_votacoes == 0:
    print("   Nenhuma votacao para processar")
else:
    # Loop: processa votacoes em batches de 10
    for batch_start in range(0, total_votacoes, BATCH_SIZE):
        # Extrai batch atual
        batch_end = min(batch_start + BATCH_SIZE, total_votacoes)
        batch = votacoes_lista[batch_start:batch_end]
        
        # Prepara lista de tuplas (endpoint, params) para fetch paralelo
        endpoints_list = [(f"/votacoes/{v['id']}/votos", {}) for v in batch]
        
        # Exibe progresso
        batch_num = batch_start//BATCH_SIZE + 1
        total_batches = (total_votacoes-1)//BATCH_SIZE + 1
        print(f"   Batch {batch_num}/{total_batches}: Processando {len(batch)} votacoes...")
        
        # Busca votos em paralelo (10 threads)
        resultados = fetch_api_parallel(
            endpoints_list=endpoints_list,
            max_workers=MAX_WORKERS
        )
        
        # Processa resultados do batch
        votos_batch = []
        for idx, resultado in enumerate(resultados):
            if resultado['success']:
                # Pega a votacao correspondente
                votacao = batch[idx]
                vot_id = votacao['id']
                
                # Para cada voto, adiciona campos de referencia
                for voto in resultado['data']:
                    voto['_votacao_id'] = vot_id
                    voto['_votacao_data'] = votacao.get('data', '')
                    voto['_sigla_orgao'] = votacao.get('siglaOrgao', '')
                
                # Acumula votos do batch
                votos_batch.extend(resultado['data'])
        
        # CHECKPOINT: Grava o batch e limpa memoria
        if votos_batch:
            mode = "overwrite" if batch_start == 0 else "append"
            n_votos = save_to_bronze(votos_batch, "votos", "/votacoes/{id}/votos", mode=mode)
            total_gravado += n_votos
            print(f"      Gravados: {n_votos} votos (Total: {total_gravado})")
            
            # Limpa memoria
            votos_batch = []
        
        # Pequena pausa entre batches
        if batch_start + BATCH_SIZE < total_votacoes:
            time.sleep(0.5)
    
    # Exibe total final
    print(f"   CONCLUIDO: {total_gravado} votos gravados no total")

# COMMAND ----------

# DBTITLE 1,Gravacao dos Dados
# MAGIC %md
# MAGIC # Gravacao na Camada Bronze

# COMMAND ----------

# DBTITLE 1,Sobre a Gravacao
# MAGIC %md
# MAGIC Na celula abaixo os dados sao gravados em modo `append` (adiciona sem apagar)
# MAGIC para preservar o historico completo de votacoes e votos.

# COMMAND ----------

# DBTITLE 1,Grava Bronze Votacoes
# ============================================================
# NOTA: GRAVACAO JA REALIZADA
# ============================================================
# A gravacao dos dados ja foi realizada nas celulas anteriores:
# - Votacoes: gravadas em lotes de 10 na celula "Ingere Votacoes"
# - Votos: gravados em lotes de 10 na celula "Ingere Votos Individuais"
# 
# Esta celula foi mantida apenas para preservar a estrutura
# do notebook e registrar no status_list.
# ============================================================

print("Dados ja gravados nas celulas anteriores.")

# Registra status para o resumo final
if 'total_gravado' in dir() and total_gravado > 0:
    status_list.append({"tabela": "ft_bronze.votacoes", "registros": len(votacoes_lista) if votacoes_lista else 0})
    status_list.append({"tabela": "ft_bronze.votos", "registros": total_gravado})
    print(f"   - ft_bronze.votacoes: {len(votacoes_lista) if votacoes_lista else 0} registros")
    print(f"   - ft_bronze.votos: {total_gravado} registros")
else:
    print("   Nenhum registro processado nesta execucao")

# COMMAND ----------

# DBTITLE 1,Atualizacao do Offset
# MAGIC %md
# MAGIC ## Atualizacao do Controle de Offset

# COMMAND ----------

# DBTITLE 1,Sobre a Atualizacao
# MAGIC %md
# MAGIC Na celula abaixo e registrado o maior ID de votacao processado nesta execucao.
# MAGIC Na proxima execucao, apenas votacoes com ID superior serao buscadas.

# COMMAND ----------

# DBTITLE 1,Atualiza Offset
# ============================================================
# ATUALIZA CONTROLE DE OFFSET
# ============================================================
# Registra o maior ID processado para proxima execucao.
# ============================================================

# Verifica se houve votacoes processadas
if votacoes_lista:
    # Busca o maior ID entre todas as votacoes processadas
    max_id = max(v['id'] for v in votacoes_lista)
    # Atualiza o watermark com o novo ID maximo
    set_watermark("votacoes", str(max_id))
    # Informa o usuario
    print(f"   Offset atualizado: ID {max_id}")
else:
    # Nenhuma votacao nova encontrada
    print("   Nenhuma votacao nova para processar")

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
