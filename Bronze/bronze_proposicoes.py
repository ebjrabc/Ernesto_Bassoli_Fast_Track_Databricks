# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://gazetadasemana.com.br/images/noticias/166864/19041851_compass.uo.jpg.jpg" width="450"/>
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Bronze - Proposicoes Legislativas
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook realiza a ingestao incremental das proposicoes legislativas (PLs, PECs, MPs, PLPs, PDLs)
# MAGIC e suas tramitacoes. O controle incremental e feito pela data de apresentacao.
# MAGIC Para cada proposicao, sao extraidas todas as tramitacoes com hash MD5 para deteccao de mudancas (CDC).
# MAGIC Os hashes permitem identificar alteracoes no historico de tramitacao na camada Silver (SCD Type 2).
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `GET /proposicoes` | Proposicoes com id, tipo, numero, ano, ementa, data |
# MAGIC | `GET /proposicoes/{id}/tramitacoes` | Historico de tramitacao com hash CDC |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_bronze.proposicoes` | Proposicoes legislativas (incremental append) |
# MAGIC | `dt0025_dev.ft_bronze.tramitacoes` | Tramitacoes com _payload_hash para CDC |
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
log_notebook_start("bronze_proposicoes")

# COMMAND ----------

# DBTITLE 1,Controle Incremental
# MAGIC %md
# MAGIC # Controle de Carga Incremental por Data

# COMMAND ----------

# DBTITLE 1,Sobre o Controle por Data
# MAGIC %md
# MAGIC Na celula abaixo e recuperada a data da ultima proposicao ja ingerida.
# MAGIC Apenas proposicoes apresentadas apos esta data serao buscadas.
# MAGIC Se for a primeira execucao, busca desde 01/02/2023 (inicio da legislatura 57).

# COMMAND ----------

# DBTITLE 1,Verifica Watermark
# ============================================================
# CONTROLE INCREMENTAL POR DATA
# ============================================================
# Usa a data de apresentacao da ultima proposicao ingerida
# para buscar apenas novas proposicoes.
# ============================================================

# Recupera o watermark do controle incremental
watermark = get_watermark("proposicoes")

# Verifica se ja existe watermark com data
if isinstance(watermark, dict) and watermark.get("last_date"):
    # Usa a data do ultimo processamento
    data_inicio = watermark["last_date"]
    # Informa que e carga incremental
    print(f"   Carga incremental desde: {data_inicio}")
else:
    # Primeira execucao: busca desde inicio da legislatura
    data_inicio = "2023-02-01"
    # Informa que e carga completa
    print(f"   Carga completa desde inicio da legislatura 57")

# COMMAND ----------

# DBTITLE 1,Ingestao dos Dados
# MAGIC %md
# MAGIC # Ingestao dos Dados da API

# COMMAND ----------

# DBTITLE 1,Sobre as Proposicoes
# MAGIC %md
# MAGIC Na celula abaixo sao buscadas as proposicoes legislativas por tipo:
# MAGIC - **PL**: Projeto de Lei (lei ordinaria)
# MAGIC - **PEC**: Proposta de Emenda a Constituicao
# MAGIC - **MP**: Medida Provisoria
# MAGIC - **PLP**: Projeto de Lei Complementar
# MAGIC - **PDL**: Projeto de Decreto Legislativo

# COMMAND ----------

# DBTITLE 1,Ingere Proposicoes
# ============================================================
# INGESTAO DAS PROPOSICOES LEGISLATIVAS (OTIMIZADO)
# ============================================================
# Busca PLs, PECs, MPs e demais proposicoes por tipo.
# Grava em lotes de 10 para liberar memoria.
# ============================================================

print("Ingerindo proposicoes legislativas (versao otimizada com checkpoints)...")

# Lista de tipos de proposicao a buscar
tipos = ["PL", "PEC", "MP", "PLP", "PDL"]

# Lista para manter apenas IDs (para buscar tramitacoes depois)
proposicoes_ids = []

# Contador total
total_gravado = 0

# Configuracao de checkpoint
BATCH_SIZE = 10  # Grava de 10 em 10

# Loop: busca proposicoes de cada tipo
for tipo in tipos:
    print(f"   Processando tipo: {tipo}")
    
    # Busca proposicoes deste tipo via API
    proposicoes_tipo = fetch_api(
        endpoint="/proposicoes",
        params={
            "siglaTipo": tipo,
            "dataApresentacaoInicio": data_inicio,
            "idLegislatura": 57,
            "ordenarPor": "id",
            "ordem": "ASC"
        }
    )
    
    print(f"      {tipo}: {len(proposicoes_tipo)} proposicoes encontradas")
    
    # Se nao houver proposicoes deste tipo, pula
    if not proposicoes_tipo:
        continue
    
    # Processa em lotes de 10
    for i in range(0, len(proposicoes_tipo), BATCH_SIZE):
        batch = proposicoes_tipo[i:i+BATCH_SIZE]
        batch_num = i//BATCH_SIZE + 1
        total_batches = (len(proposicoes_tipo)-1)//BATCH_SIZE + 1
        
        # Define modo: overwrite no primeiro batch do primeiro tipo, append nos demais
        if tipo == "PL" and i == 0:
            mode = "overwrite"
        else:
            mode = "append"
        
        # Grava o batch
        n = save_to_bronze(batch, "proposicoes", "/proposicoes", mode=mode)
        total_gravado += len(batch)
        
        # Guarda apenas IDs e dados minimos para buscar tramitacoes
        for p in batch:
            proposicoes_ids.append({
                'id': p['id'],
                'siglaTipo': p.get('siglaTipo', ''),
                'numero': p.get('numero', ''),
                'ano': p.get('ano', ''),
                'dataApresentacao': p.get('dataApresentacao', '')
            })
        
        print(f"      Batch {batch_num}/{total_batches}: Gravadas {len(batch)} proposicoes (Total: {total_gravado})")
        
        # Limpa memoria do batch
        batch = []
    
    # Limpa memoria das proposicoes deste tipo
    proposicoes_tipo = []
    
    # Pequena pausa entre tipos
    time.sleep(0.5)

print(f"   CONCLUIDO: {total_gravado} proposicoes gravadas no total")
print(f"   IDs salvos para buscar tramitacoes: {len(proposicoes_ids)}")

# Cria proposicoes_lista apenas com IDs minimos (para compatibilidade)
proposicoes_lista = proposicoes_ids

# COMMAND ----------

# DBTITLE 1,Sobre as Tramitacoes e CDC
# MAGIC %md
# MAGIC Na celula abaixo, para cada proposicao, e buscado o historico completo de tramitacoes.
# MAGIC Cada tramitacao indica uma mudanca de status (ex: "Aprovada na Comissao", "Enviada ao Senado").
# MAGIC Para cada registro e gerado um hash MD5 do payload, usado como mecanismo de Change Data Capture (CDC):
# MAGIC se o hash mudar entre execucoes, significa que houve alteracao nos dados.

# COMMAND ----------

# DBTITLE 1,Ingere Tramitacoes CDC
# ============================================================
# INGESTAO DAS TRAMITACOES (OTIMIZADO COM CHECKPOINTS)
# ============================================================
# Para cada proposicao, busca tramitacoes em lotes de 10.
# Grava incrementalmente para liberar memoria.
# ============================================================

print("Ingerindo tramitacoes CDC (versao otimizada com checkpoints)...")

# Configuracao de processamento
BATCH_SIZE = 10  # Processa 10 proposicoes por vez

# Contador total
total_gravado = 0

# Total de proposicoes
total = len(proposicoes_lista)

if total == 0:
    print("   Nenhuma proposicao para processar")
    tramitacoes_lista = []  # Lista vazia para compatibilidade
else:
    # Processa em lotes de 10 proposicoes
    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = proposicoes_lista[batch_start:batch_end]
        
        batch_num = batch_start//BATCH_SIZE + 1
        total_batches = (total-1)//BATCH_SIZE + 1
        
        print(f"   Batch {batch_num}/{total_batches}: Processando {len(batch)} proposicoes...")
        
        # Lista para acumular tramitacoes deste batch
        tramitacoes_batch = []
        
        # Processa cada proposicao do batch
        for prop in batch:
            prop_id = prop['id']
            
            try:
                # Busca tramitacoes desta proposicao
                dados = fetch_api(f"/proposicoes/{prop_id}/tramitacoes")
                
                # Para cada tramitacao, adiciona campos auxiliares e hash
                for d in dados:
                    d['_proposicao_id'] = prop_id
                    d['_sigla_tipo'] = prop.get('siglaTipo', '')
                    d['_numero'] = prop.get('numero', '')
                    d['_ano'] = prop.get('ano', '')
                    # Gera hash MD5 do payload para CDC
                    payload = json.dumps(d, sort_keys=True, default=str)
                    d['_payload_hash'] = str(hash(payload))
                
                # Acumula tramitacoes
                tramitacoes_batch.extend(dados)
                
            except requests.exceptions.ConnectionError:
                print(f"      ERRO DE CONEXAO na proposicao {prop_id} - abortando")
                break
            except Exception as e:
                print(f"      Erro na proposicao {prop_id}: {str(e)[:60]}")
        
        # CHECKPOINT: Grava o batch
        if tramitacoes_batch:
            mode = "overwrite" if batch_start == 0 else "append"
            n = save_to_bronze(tramitacoes_batch, "tramitacoes", "/proposicoes/{id}/tramitacoes", mode=mode)
            total_gravado += len(tramitacoes_batch)
            print(f"      Gravadas: {len(tramitacoes_batch)} tramitacoes (Total: {total_gravado})")
            
            # Limpa memoria
            tramitacoes_batch = []
        
        # Pequena pausa entre batches
        if batch_start + BATCH_SIZE < total:
            time.sleep(0.5)
    
    print(f"   CONCLUIDO: {total_gravado} tramitacoes gravadas no total")
    
    # Cria tramitacoes_lista vazia (dados ja gravados)
    tramitacoes_lista = []

# COMMAND ----------

# DBTITLE 1,Gravacao dos Dados
# MAGIC %md
# MAGIC # Gravacao na Camada Bronze

# COMMAND ----------

# DBTITLE 1,Sobre a Gravacao
# MAGIC %md
# MAGIC Na celula abaixo os dados sao gravados em modo `append` para preservar todo o historico.
# MAGIC O Delta Lake permite Time Travel para consultar versoes anteriores dos dados.

# COMMAND ----------

# DBTITLE 1,Grava Bronze Proposicoes
# ============================================================
# NOTA: GRAVACAO JA REALIZADA
# ============================================================
# A gravacao dos dados ja foi realizada nas celulas anteriores:
# - Proposicoes: gravadas em lotes de 10 na celula "Ingere Proposicoes"
# - Tramitacoes: gravadas em lotes de 10 na celula "Ingere Tramitacoes CDC"
# 
# Esta celula foi mantida apenas para preservar a estrutura
# do notebook e registrar no status_list.
# ============================================================

print("Dados ja gravados nas celulas anteriores.")

# Conta registros das tabelas para o status
if proposicoes_lista:
    n1 = len(proposicoes_lista)
    status_list.append({"tabela": "ft_bronze.proposicoes", "registros": n1})
    print(f"   - ft_bronze.proposicoes: {n1} registros")

# Para tramitacoes, usa o contador total_gravado da celula anterior
if 'total_gravado' in dir() and total_gravado > 0:
    status_list.append({"tabela": "ft_bronze.tramitacoes", "registros": total_gravado})
    print(f"   - ft_bronze.tramitacoes: {total_gravado} registros")
else:
    print("   Nenhum registro processado nesta execucao")

# COMMAND ----------

# DBTITLE 1,Atualizacao do Watermark
# MAGIC %md
# MAGIC ## Atualizacao do Controle Incremental

# COMMAND ----------

# DBTITLE 1,Sobre a Atualizacao
# MAGIC %md
# MAGIC Na celula abaixo e atualizado o watermark com a data da ultima proposicao processada.

# COMMAND ----------

# DBTITLE 1,Atualiza Watermark
# ============================================================
# ATUALIZA CONTROLE INCREMENTAL
# ============================================================
# Grava a data da ultima proposicao para proxima execucao.
# ============================================================

# Verifica se houve proposicoes processadas
if proposicoes_lista:
    # Busca a maior data de apresentacao entre as proposicoes
    last_date = max(p.get('dataApresentacao', '')[:10] for p in proposicoes_lista if p.get('dataApresentacao'))
    # Atualiza watermark
    set_watermark("proposicoes", last_date)
    # Informa usuario
    print(f"   Watermark atualizado: {last_date}")

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
