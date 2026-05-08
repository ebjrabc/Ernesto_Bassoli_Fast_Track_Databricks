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
# INGESTAO DAS VOTACOES NOMINAIS
# ============================================================
# Busca votacoes do plenario e comissoes. Cada votacao
# contem: id, data, orgao, proposicao relacionada,
# descricao e resultado.
# ============================================================

# Informa o usuario que a ingestao esta iniciando
print("Ingerindo votacoes...")

# Busca votacoes da API com paginacao automatica
votacoes_lista = fetch_api(
    endpoint="/votacoes",
    params=params_votacoes
)

# Filtra apenas votacoes novas (ID maior que o watermark)
if isinstance(watermark, dict) and watermark.get("last_id"):
    # Mantem apenas votacoes com ID posterior ao ultimo processado
    votacoes_lista = [v for v in votacoes_lista if str(v['id']) > str(watermark['last_id'])]

# Exibe quantidade de votacoes novas encontradas
print(f"   Votacoes novas: {len(votacoes_lista)}")

# COMMAND ----------

# DBTITLE 1,Sobre os Votos Individuais
# MAGIC %md
# MAGIC Na celula abaixo, para cada votacao, sao buscados os votos individuais de cada deputado.
# MAGIC Cada voto contem: deputado, partido, tipo do voto (Sim/Nao/Abstencao/Obstrucao).
# MAGIC Esses dados sao essenciais para analise de coesao partidaria e correlacao com frentes.

# COMMAND ----------

# DBTITLE 1,Ingere Votos Individuais
# ============================================================
# INGESTAO DOS VOTOS DE CADA DEPUTADO
# ============================================================
# Para cada votacao, busca os votos individuais.
# Essencial para analise de coesao partidaria
# e correlacao com frentes parlamentares.
# ============================================================

# Informa o usuario que a ingestao de votos esta iniciando
print("Ingerindo votos individuais...")

# Lista para acumular todos os votos
votos_lista = []

# Total de votacoes para calculo de progresso
total = len(votacoes_lista)

# Loop: percorre cada votacao para buscar votos individuais
for i, votacao in enumerate(votacoes_lista):
    # Extrai o ID da votacao atual
    vot_id = votacao['id']
    
    try:
        # Busca votos individuais desta votacao
        dados = fetch_api(f"/votacoes/{vot_id}/votos")
        
        # Para cada voto, adiciona campos de referencia
        for d in dados:
            # Adiciona o ID da votacao como campo auxiliar
            d['_votacao_id'] = vot_id
            # Adiciona a data da votacao
            d['_votacao_data'] = votacao.get('data', votacao.get('dataHoraRegistro', ''))
            # Adiciona a sigla do orgao onde ocorreu
            d['_sigla_orgao'] = votacao.get('siglaOrgao', '')
        
        # Adiciona os votos desta votacao a lista geral
        votos_lista.extend(dados)
        
    # Erro de conexao (rede indisponivel)
    except requests.exceptions.ConnectionError:
        # Informa o usuario e interrompe o loop
        print(f"  ERRO DE CONEXAO na votacao {vot_id} - abortando")
        break
        
    # Qualquer outro erro
    except Exception as e:
        # Informa o usuario e continua com a proxima votacao
        print(f"  Erro na votacao {vot_id}: {str(e)[:60]}")
    
    # A cada 50 votacoes, exibe progresso
    if (i + 1) % 50 == 0:
        print(f"   Progresso: {i+1}/{total} ({(i+1)*100//total}%)")
        time.sleep(0.3)

# Exibe total de votos obtidos
print(f"   Total votos: {len(votos_lista)} registros")

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
# GRAVACAO NA CAMADA BRONZE
# ============================================================
# Grava votacoes e votos em modo append para preservar
# historico completo.
# ============================================================

# Grava votacoes na tabela bronze (append)
n1 = save_to_bronze(votacoes_lista, "votacoes", "/votacoes", mode="append")

# Registra status para o resumo final
status_list.append({"tabela": "ft_bronze.votacoes", "registros": n1})

# Grava votos individuais na tabela bronze (append)
n2 = save_to_bronze(votos_lista, "votos", "/votacoes/{id}/votos", mode="append")

# Registra status para o resumo final
status_list.append({"tabela": "ft_bronze.votos", "registros": n2})

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
