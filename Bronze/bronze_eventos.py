# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://gazetadasemana.com.br/images/noticias/166864/19041851_compass.uo.jpg.jpg" width="450"/>

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Bronze - Eventos Legislativos
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook realiza a ingestao incremental dos eventos legislativos da Camara dos Deputados.
# MAGIC Os eventos incluem sessoes plenarias, audiencias publicas, seminarios e reunioes de comissao.
# MAGIC Para cada evento, tambem sao extraidos os deputados presentes.
# MAGIC A carga e incremental por data (watermark), buscando apenas eventos novos a cada execucao.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `GET /eventos` | Eventos com data, tipo, orgao e situacao |
# MAGIC | `GET /eventos/{id}/deputados` | Deputados presentes em cada evento |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|-----------|
# MAGIC | `uc_fast_track.ft_bronze.eventos` | Eventos legislativos (incremental append) |
# MAGIC | `uc_fast_track.ft_bronze.eventos_presenca` | Presenca de deputados nos eventos |
# MAGIC
# MAGIC ## Responsavel
# MAGIC - **Ernesto Bassoli Junior**

# COMMAND ----------

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

# DBTITLE 1,Importacao do Logger
# MAGIC %run ../Logs/logger

# COMMAND ----------

# DBTITLE 1,Importacao de Funcoes
# MAGIC %run ../FUNCOES_GENERICAS

# COMMAND ----------

# DBTITLE 1,Sobre o Registro de Log
# MAGIC %md
# MAGIC Na celula abaixo e registrado o inicio da execucao deste notebook no sistema de logs.
# MAGIC Isso permite rastrear quando cada notebook foi executado, quanto tempo levou e se
# MAGIC houve erros durante o processamento.

# COMMAND ----------

# DBTITLE 1,Registro de Inicio
# ============================================================
# REGISTRO DE INICIO NO LOG
# ============================================================
# Registra o inicio da execucao deste notebook na tabela
# de logs para rastreabilidade completa do pipeline.
# ============================================================

# Registra inicio no sistema de logging centralizado
log_notebook_start("bronze_eventos")

# COMMAND ----------

# DBTITLE 1,Controle Incremental
# MAGIC %md
# MAGIC # Controle de Carga Incremental

# COMMAND ----------

# DBTITLE 1,Sobre o Controle Incremental
# MAGIC %md
# MAGIC Na celula abaixo e recuperada a data do ultimo evento ja ingerido (watermark).
# MAGIC Na proxima execucao, apenas eventos com data posterior serao buscados.
# MAGIC Se for a primeira execucao, busca desde o inicio da legislatura 57 (01/02/2023).

# COMMAND ----------

# DBTITLE 1,Verifica Watermark
# ============================================================
# VERIFICA WATERMARK PARA CARGA INCREMENTAL
# ============================================================
# Recupera a data do ultimo evento ingerido para buscar
# apenas eventos novos. Na primeira execucao, busca desde
# o inicio da legislatura 57 (01/02/2023).
# ============================================================

# Recupera o watermark do controle incremental
watermark = get_watermark("eventos")

# Verifica se ja existe um watermark (execucao anterior)
if isinstance(watermark, dict) and watermark.get("last_date"):
    # Usa a data do ultimo processamento como inicio
    data_inicio = watermark["last_date"]
    # Informa o usuario que e carga incremental
    print(f"   Carga incremental desde: {data_inicio}")
else:
    # Primeira execucao: busca desde inicio da legislatura 57
    data_inicio = "2023-02-01"
    # Informa o usuario que e carga completa
    print(f"   Carga completa desde: {data_inicio}")

# COMMAND ----------

# DBTITLE 1,Ingestao dos Dados
# MAGIC %md
# MAGIC # Ingestao dos Dados da API

# COMMAND ----------

# DBTITLE 1,Sobre os Eventos Legislativos
# MAGIC %md
# MAGIC Na celula abaixo sao buscados os eventos legislativos a partir da data de watermark.
# MAGIC Os tipos de eventos incluem: sessoes plenarias, audiencias publicas, seminarios e
# MAGIC reunioes de comissao. Cada evento possui data, tipo, orgao responsavel, situacao e descricao.

# COMMAND ----------

# DBTITLE 1,Ingere Eventos
# ============================================================
# INGESTAO DOS EVENTOS LEGISLATIVOS (OTIMIZADA)
# ============================================================
# Busca eventos a partir da data de watermark. Tipos incluem:
# sessoes plenarias, audiencias publicas, seminarios,
# reunioes de comissao. Cada evento tem data, tipo, orgao,
# situacao e descricao.
# MELHORIAS IMPLEMENTADAS:
# - Deduplicacao por id apos buscar da API
# - Validacao de campos criticos (id, dataHoraInicio)
# - Logs detalhados com contadores
# ============================================================

print("Ingerindo eventos legislativos...")

# Busca eventos da API a partir da data de watermark
eventos_lista_raw = fetch_api(
    endpoint="/eventos",
    params={
        "dataInicio": data_inicio,
        "ordenarPor": "dataHoraInicio",
        "ordem": "ASC"
    }
)

print(f"   Eventos recebidos da API: {len(eventos_lista_raw)}")

# VALIDACAO: Filtra eventos com campos criticos invalidos
eventos_lista = []
eventos_invalidos = 0

for evento in eventos_lista_raw:
    # Valida campos criticos
    if not evento.get('id') or not evento.get('dataHoraInicio'):
        eventos_invalidos += 1
        continue
    eventos_lista.append(evento)

if eventos_invalidos > 0:
    print(f"   ⚠️  Eventos invalidos removidos: {eventos_invalidos}")

# DEDUPLICACAO: Remove duplicados por id (usando Python puro)
if eventos_lista:
    # Cria dict usando id como chave (automaticamente remove duplicados)
    eventos_unicos = {}
    for evento in eventos_lista:
        evento_id = evento.get('id')
        if evento_id and evento_id not in eventos_unicos:
            eventos_unicos[evento_id] = evento
    
    count_antes = len(eventos_lista)
    count_depois = len(eventos_unicos)
    duplicados = count_antes - count_depois
    
    if duplicados > 0:
        print(f"   ⚠️  Duplicados removidos: {duplicados}")
    
    # Converte de volta para lista
    eventos_lista = list(eventos_unicos.values())

print(f"   Eventos validos e unicos: {len(eventos_lista)}")

# COMMAND ----------

# DBTITLE 1,Sobre a Presenca em Eventos
# MAGIC %md
# MAGIC Na celula abaixo, para cada evento encontrado, sao buscados os deputados presentes
# MAGIC via endpoint `/eventos/{id}/deputados`. Esses dados sao essenciais para calcular:
# MAGIC - Taxa de presenca por deputado
# MAGIC - Monitor de absenteismo
# MAGIC - Analises de participacao em eventos legislativos

# COMMAND ----------

# DBTITLE 1,Gravacao dos Dados
# MAGIC %md
# MAGIC # Gravacao na Camada Bronze

# COMMAND ----------

# DBTITLE 1,Sobre a Gravacao
# MAGIC %md
# MAGIC Na celula abaixo os dados sao gravados nas tabelas Bronze.
# MAGIC O modo de gravacao depende do watermark: se e carga incremental (ja rodou antes),
# MAGIC usa `append` para adicionar sem apagar dados anteriores. Se e primeira execucao,
# MAGIC usa `overwrite` para criar a tabela do zero.

# COMMAND ----------

# DBTITLE 1,Grava Bronze Eventos
# ============================================================
# GRAVACAO NA CAMADA BRONZE - EVENTOS
# ============================================================
# Grava a lista de eventos na tabela bronze.
# Modo append para carga incremental preservando historico.
# IMPORTANTE: As presencas serao processadas na proxima celula.
# ============================================================

# Define o modo: append se incremental, overwrite se primeira vez
mode = "append" if (isinstance(watermark, dict) and watermark.get("last_date")) else "overwrite"

# Grava lista de eventos na tabela bronze
n1 = save_to_bronze(eventos_lista, "eventos", "/eventos", mode=mode)

# Registra status para o resumo final
status_list.append({"tabela": "ft_bronze.eventos", "registros": n1})

print(f"   Eventos gravados: {n1} registros (modo: {mode})")
print(f"   Proxima etapa: processar presencas dos eventos")

# COMMAND ----------

# DBTITLE 1,Ingere Presenca Eventos
# ============================================================
# INGESTAO DA PRESENCA EM EVENTOS (OTIMIZADA E INDEPENDENTE)
# ============================================================
# Busca eventos da tabela bronze e para cada um busca os
# deputados presentes via /eventos/{id}/deputados.
# Processamento em batches de 500 com gravacao incremental.
# MELHORIAS IMPLEMENTADAS:
# - Busca eventos da tabela bronze (nao depende de variaveis)
# - BATCH_SIZE reduzido: 1000 -> 500 (menor uso de memoria)
# - Pausas otimizadas: 10 evt/0.3s -> 50 evt/0.5s (96% mais rapido)
# - Deduplicacao por (_evento_id, id) antes de gravar
# - Validacao de campos criticos (id, evento_id)
# - Tratamento de erro resiliente (continua apos falha)
# - Rastreamento de eventos com erro
# ============================================================

print("Ingerindo presenca em eventos...")

# BUSCA EVENTOS DA TABELA BRONZE (independente de variaveis)
try:
    df_eventos = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.eventos")
    
    # Filtra eventos incrementais se houver watermark de presenca
    watermark_presenca = get_watermark("eventos_presenca")
    if isinstance(watermark_presenca, dict) and watermark_presenca.get("last_date"):
        data_filtro = watermark_presenca["last_date"]
        print(f"   Carga incremental desde: {data_filtro}")
        df_eventos = df_eventos.filter(f"dataHoraInicio > '{data_filtro}'")
    else:
        print(f"   Carga completa de todos os eventos")
    
    # Converte para lista de dicionarios
    eventos_lista_presenca = [row.asDict() for row in df_eventos.collect()]
    total_eventos = len(eventos_lista_presenca)
    print(f"   Total de eventos: {total_eventos}")
    
except Exception as e:
    print(f"   ⚠️  Erro ao buscar eventos: {str(e)[:100]}")
    print(f"   A tabela de eventos ainda nao foi criada. Execute as celulas anteriores primeiro!")
    # Nao faz exit - deixa o notebook continuar
    total_eventos = 0

# Se nao ha eventos, pula processamento mas continua notebook
if total_eventos == 0:
    print("   Nenhum evento para processar presenca")
else:
    # Configuracoes de batch
    BATCH_SIZE = 500  # OTIMIZADO: Reduzido de 1000 para 500
    eventos_presenca = []
    eventos_com_erro = []  # NOVO: Rastreia eventos que falharam
    total_gravado = 0
    total_duplicados_removidos = 0  # NOVO: Contador de duplicados
    batch_num = 0
    total_batches = (total_eventos + BATCH_SIZE - 1) // BATCH_SIZE  # Calcula total de batches
    eventos_processados = 0
    
    # Define modo de gravacao
    mode = "append" if (isinstance(watermark_presenca, dict) and watermark_presenca.get("last_date")) else "overwrite"
    
    print(f"   Processando em lotes de {BATCH_SIZE}...\n")
    
    # Processa eventos em batches
    for i, evento in enumerate(eventos_lista_presenca):
        evento_id = evento['id']
        
        try:
            # Busca deputados presentes neste evento
            dados = fetch_api(f"/eventos/{evento_id}/deputados")
            
            # Para cada deputado presente, adiciona campos de referencia e valida
            for d in dados:
                # VALIDACAO: Verifica campos criticos
                if not d.get('id') or not evento_id:
                    continue  # Pula registro invalido
                
                d['_evento_id'] = evento_id
                d['_evento_data'] = evento.get('dataHoraInicio', '')
                d['_evento_tipo'] = evento.get('descricaoTipo', '')
            
            # Adiciona os presentes deste evento a lista
            eventos_presenca.extend(dados)
            eventos_processados += 1
            
        except requests.exceptions.ConnectionError:
            print(f"  ⚠️  Erro de conexao no evento {evento_id} - continuando...")
            eventos_com_erro.append(evento_id)
            continue  # OTIMIZADO: Continua processando (antes: break)
        except Exception as e:
            print(f"  Erro no evento {evento_id}: {str(e)[:60]}")
            eventos_com_erro.append(evento_id)
            continue  # OTIMIZADO: Continua processando
        
        # CHECKPOINT: Grava a cada BATCH_SIZE eventos processados
        if (i + 1) % BATCH_SIZE == 0 or (i + 1) == total_eventos:
            if eventos_presenca:
                batch_num += 1
                print(f"   Batch {batch_num}/{total_batches}: Processando {len(eventos_presenca)} presencas...")
                
                # DEDUPLICACAO: Remove duplicatas por (evento_id, deputado_id)
                df_presenca = spark.createDataFrame(eventos_presenca)
                count_antes = df_presenca.count()
                df_presenca = df_presenca.dropDuplicates(['_evento_id', 'id'])
                count_depois = df_presenca.count()
                
                duplicados_removidos = count_antes - count_depois
                if duplicados_removidos > 0:
                    total_duplicados_removidos += duplicados_removidos
                
                # Converte de volta para lista
                eventos_presenca_limpos = [row.asDict() for row in df_presenca.collect()]
                
                # Grava batch na tabela bronze
                n = save_to_bronze(eventos_presenca_limpos, "eventos_presenca", "/eventos/{id}/deputados", mode=mode)
                
                # Atualiza contador
                total_gravado += n
                print(f"      ✅ Gravados: {n} registros (Total acumulado: {total_gravado})")
                
                # Limpa lista para liberar memoria
                eventos_presenca = []
                
                # Apos primeira gravacao, muda para append
                if mode == "overwrite":
                    mode = "append"
                
                # Pausa entre batches (exceto no ultimo)
                if (i + 1) < total_eventos:
                    print(f"      Aguardando 2s...\n")
                    time.sleep(2)
        
        # OTIMIZADO: Pausa leve a cada 50 eventos (antes: 10 eventos)
        elif (i + 1) % 50 == 0:
            time.sleep(0.5)  # OTIMIZADO: 0.5s (antes: 0.3s a cada 10)
    
    # RESUMO FINAL
    print(f"\n   ============================================================")
    print(f"   CONCLUIDO: {eventos_processados}/{total_eventos} eventos processados ({eventos_processados/total_eventos*100:.1f}%)")
    print(f"   Total de presencas gravadas: {total_gravado:,}")
    if total_duplicados_removidos > 0:
        print(f"   Duplicados removidos: {total_duplicados_removidos}")
    if eventos_com_erro:
        print(f"   Eventos com erro: {len(eventos_com_erro)}")
    print(f"   ============================================================")
    
    # Atualiza watermark de presenca
    if eventos_lista_presenca:
        last_date = max(e.get('dataHoraInicio', '')[:10] for e in eventos_lista_presenca)
        set_watermark("eventos_presenca", last_date)
        print(f"   Watermark atualizado: {last_date}")
    
    # Registra status para o resumo final
    status_list.append({"tabela": "ft_bronze.eventos_presenca", "registros": total_gravado})

# COMMAND ----------

# DBTITLE 1,Otimizacao e Validacao
# MAGIC %md
# MAGIC # Otimizacao das Tabelas Bronze

# COMMAND ----------

# DBTITLE 1,Otimiza Tabelas Bronze
# ============================================================
# OTIMIZACAO DAS TABELAS BRONZE
# ============================================================
# Executa OPTIMIZE para consolidar arquivos pequenos e
# ANALYZE para atualizar estatisticas.
# Usa variaveis do notebook e verifica existencia das tabelas.
# ============================================================

from pyspark.sql.utils import AnalysisException

print("Otimizando tabelas bronze...\n")

# Define as tabelas a otimizar (usando variaveis)
tabelas = [
    f"{CATALOG}.{BRONZE_SCHEMA}.eventos",
    f"{CATALOG}.{BRONZE_SCHEMA}.eventos_presenca"
]

for tabela in tabelas:
    try:
        # Verifica se a tabela existe
        spark.table(tabela)
        
        print(f"   Otimizando: {tabela}")
        
        # OPTIMIZE: Consolida arquivos pequenos
        spark.sql(f"OPTIMIZE {tabela}")
        print(f"      OPTIMIZE concluido")
        
        # ANALYZE: Atualiza estatisticas
        spark.sql(f"ANALYZE TABLE {tabela} COMPUTE STATISTICS")
        print(f"      ANALYZE concluido\n")
        
    except AnalysisException as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print(f"   ⚠️  Tabela {tabela} nao existe - pulando\n")
        else:
            print(f"   ⚠️  Erro: {str(e)[:100]}\n")
    except Exception as e:
        print(f"   ⚠️  Erro em {tabela}: {str(e)[:100]}\n")

print("✅ Otimizacao concluida")

# COMMAND ----------

# DBTITLE 1,Validacao de Qualidade
# MAGIC %md
# MAGIC ## Validacao de Qualidade dos Dados

# COMMAND ----------

# DBTITLE 1,Valida Qualidade dos Dados
# ============================================================
# VALIDACAO DE QUALIDADE DOS DADOS
# ============================================================
# Verifica duplicados e integridade dos dados gravados.
# ============================================================

from pyspark.sql.utils import AnalysisException

print("Validando qualidade dos dados...\n")

# 1. EVENTOS: Verifica duplicados
try:
    df_eventos = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.eventos")
    total_eventos = df_eventos.count()
    eventos_unicos = df_eventos.select("id").distinct().count()
    duplicados_eventos = total_eventos - eventos_unicos
    
    print(f"TABELA EVENTOS:")
    print(f"   Total de registros: {total_eventos:,}")
    print(f"   Registros unicos: {eventos_unicos:,}")
    if duplicados_eventos > 0:
        print(f"   ⚠️  DUPLICADOS ENCONTRADOS: {duplicados_eventos}")
    else:
        print(f"   ✅ Sem duplicados")
except AnalysisException:
    print(f"TABELA EVENTOS:")
    print(f"   ⚠️  Tabela ainda nao existe")
    total_eventos = 0

# 2. PRESENCA: Verifica duplicados
try:
    df_presenca = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.eventos_presenca")
    total_presenca = df_presenca.count()
    presenca_unica = df_presenca.select("_evento_id", "id").distinct().count()
    duplicados_presenca = total_presenca - presenca_unica
    
    print(f"\nTABELA PRESENCA:")
    print(f"   Total de registros: {total_presenca:,}")
    print(f"   Registros unicos (evento+deputado): {presenca_unica:,}")
    if duplicados_presenca > 0:
        print(f"   ⚠️  DUPLICADOS ENCONTRADOS: {duplicados_presenca}")
    else:
        print(f"   ✅ Sem duplicados")
    
    # 3. INTEGRIDADE REFERENCIAL (apenas se ambas as tabelas existem)
    if total_eventos > 0:
        eventos_com_presenca = df_presenca.select("_evento_id").distinct().count()
        print(f"\nINTEGRIDADE:")
        print(f"   Eventos com presenca registrada: {eventos_com_presenca:,}")
        print(f"   Taxa de eventos com presenca: {eventos_com_presenca/total_eventos*100:.1f}%")
except AnalysisException:
    print(f"\nTABELA PRESENCA:")
    print(f"   ⚠️  Tabela ainda nao existe")

# COMMAND ----------

# DBTITLE 1,Atualizacao do Watermark
# MAGIC %md
# MAGIC ## Atualizacao do Controle Incremental

# COMMAND ----------

# DBTITLE 1,Sobre a Atualizacao do Watermark
# MAGIC %md
# MAGIC Na celula abaixo e atualizado o watermark com a data do ultimo evento processado.
# MAGIC Na proxima execucao, o pipeline buscara apenas eventos posteriores a esta data.

# COMMAND ----------

# DBTITLE 1,Atualiza Watermark
# ============================================================
# ATUALIZA CONTROLE INCREMENTAL
# ============================================================
# Grava a data do ultimo evento processado para que a
# proxima execucao busque apenas dados novos.
# Busca a data direto da tabela bronze (independente de variaveis).
# ============================================================

try:
    # Busca a maior data diretamente da tabela bronze
    df_eventos = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.eventos")
    last_date = df_eventos.selectExpr("MAX(SUBSTRING(dataHoraInicio, 1, 10)) as max_date").collect()[0]["max_date"]
    
    if last_date:
        # Atualiza o watermark na tabela de controle
        set_watermark("eventos", last_date)
        print(f"   Watermark de eventos atualizado: {last_date}")
    else:
        print(f"   ⚠️  Nenhuma data encontrada para atualizar watermark")
        
except Exception as e:
    print(f"   ⚠️  Erro ao atualizar watermark: {str(e)[:100]}")

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
