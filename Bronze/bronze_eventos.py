# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Bronze - Eventos Legislativos
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook realiza a ingestao incremental dos eventos legislativos da Camara.
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
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_bronze.eventos` | Eventos legislativos (incremental append) |
# MAGIC | `dt0025_dev.ft_bronze.eventos_presenca` | Presenca de deputados nos eventos |
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
# INGESTAO DOS EVENTOS LEGISLATIVOS
# ============================================================
# Busca eventos a partir da data de watermark. Tipos incluem:
# sessoes plenarias, audiencias publicas, seminarios,
# reunioes de comissao. Cada evento tem data, tipo, orgao,
# situacao e descricao.
# ============================================================

# Informa o usuario que a ingestao esta iniciando
print("Ingerindo eventos legislativos...")

# Busca eventos da API a partir da data de watermark
eventos_lista = fetch_api(
    endpoint="/eventos",
    params={
        "dataInicio": data_inicio,
        "ordenarPor": "dataHoraInicio",
        "ordem": "ASC"
    }
)

# Exibe quantidade de eventos encontrados
print(f"   Eventos encontrados: {len(eventos_lista)}")

# COMMAND ----------

# DBTITLE 1,Sobre a Presenca em Eventos
# MAGIC %md
# MAGIC Na celula abaixo, para cada evento encontrado, sao buscados os deputados presentes
# MAGIC via endpoint `/eventos/{id}/deputados`. Esses dados sao essenciais para calcular:
# MAGIC - Taxa de presenca por deputado
# MAGIC - Monitor de absenteismo
# MAGIC - Score de engajamento (presenca 40% + votos 60%)

# COMMAND ----------

# DBTITLE 1,Ingere Presenca Eventos
# ============================================================
# INGESTAO DA PRESENCA EM EVENTOS
# ============================================================
# Para cada evento, busca os deputados presentes via
# /eventos/{id}/deputados. Dados essenciais para o calculo
# do score de engajamento e monitor de presenca.
# ============================================================

# Informa o usuario que a ingestao de presenca esta iniciando
print("Ingerindo presenca em eventos...")

# Lista para acumular registros de presenca
eventos_presenca = []

# Total de eventos para calculo de progresso
total = len(eventos_lista)

# Loop: percorre cada evento para buscar deputados presentes
for i, evento in enumerate(eventos_lista):
    # Extrai o ID do evento atual
    evento_id = evento['id']
    
    try:
        # Busca deputados presentes neste evento
        dados = fetch_api(f"/eventos/{evento_id}/deputados")
        
        # Para cada deputado presente, adiciona campos de referencia
        for d in dados:
            # Adiciona o ID do evento como campo auxiliar
            d['_evento_id'] = evento_id
            # Adiciona a data/hora do evento para facilitar analises
            d['_evento_data'] = evento.get('dataHoraInicio', '')
            # Adiciona o tipo do evento (plenaria, comissao, etc)
            d['_evento_tipo'] = evento.get('descricaoTipo', '')
        
        # Adiciona os presentes deste evento a lista geral
        eventos_presenca.extend(dados)
        
    # Erro de conexao (rede indisponivel)
    except requests.exceptions.ConnectionError:
        # Informa o usuario e interrompe o loop
        print(f"  ERRO DE CONEXAO no evento {evento_id} - abortando lote")
        break
        
    # Qualquer outro erro
    except Exception as e:
        # Informa o usuario e continua com o proximo evento
        print(f"  Erro no evento {evento_id}: {str(e)[:60]}")
    
    # A cada 100 eventos, exibe progresso
    if (i + 1) % 100 == 0:
        # Calcula e exibe percentual concluido
        print(f"   Progresso: {i+1}/{total} ({(i+1)*100//total}%)")
        # Pequena pausa para nao sobrecarregar a API
        time.sleep(0.3)

# Exibe total de registros de presenca obtidos
print(f"   Total presencas: {len(eventos_presenca)} registros")

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
# GRAVACAO NA CAMADA BRONZE
# ============================================================
# Grava eventos e presencas em tabelas separadas.
# Modo append para carga incremental preservando historico.
# ============================================================

# Define o modo: append se incremental, overwrite se primeira vez
mode = "append" if (isinstance(watermark, dict) and watermark.get("last_date")) else "overwrite"

# Grava lista de eventos na tabela bronze
n1 = save_to_bronze(eventos_lista, "eventos", "/eventos", mode=mode)

# Registra status para o resumo final
status_list.append({"tabela": "ft_bronze.eventos", "registros": n1})

# Grava presencas na tabela bronze
n2 = save_to_bronze(eventos_presenca, "eventos_presenca", "/eventos/{id}/deputados", mode=mode)

# Registra status para o resumo final
status_list.append({"tabela": "ft_bronze.eventos_presenca", "registros": n2})

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
# ============================================================

# Verifica se houve eventos processados nesta execucao
if eventos_lista:
    # Busca a maior data entre todos os eventos processados
    last_date = max(e.get('dataHoraInicio', '')[:10] for e in eventos_lista)
    # Atualiza o watermark na tabela de controle
    set_watermark("eventos", last_date)
    # Informa o usuario
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

