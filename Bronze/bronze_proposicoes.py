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
# INGESTAO DAS PROPOSICOES LEGISLATIVAS
# ============================================================
# Busca PLs, PECs, MPs e demais proposicoes por tipo.
# Cada registro contem: id, tipo, numero, ano, ementa.
# ============================================================

# Informa o usuario que a ingestao esta iniciando
print("Ingerindo proposicoes legislativas...")

# Lista de tipos de proposicao a buscar
tipos = ["PL", "PEC", "MP", "PLP", "PDL"]

# Lista para acumular todas as proposicoes
proposicoes_lista = []

# Loop: busca proposicoes de cada tipo separadamente
for tipo in tipos:
    # Busca proposicoes deste tipo via API
    dados = fetch_api(
        endpoint="/proposicoes",
        params={
            "siglaTipo": tipo,
            "dataApresentacaoInicio": data_inicio,
            "idLegislatura": 57,
            "ordenarPor": "id",
            "ordem": "ASC"
        }
    )
    # Adiciona a lista geral
    proposicoes_lista.extend(dados)
    # Exibe quantidade por tipo
    print(f"   {tipo}: {len(dados)} proposicoes")

# Exibe total geral
print(f"   Total proposicoes: {len(proposicoes_lista)}")

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
# INGESTAO DAS TRAMITACOES (BASE PARA CDC)
# ============================================================
# Para cada proposicao, busca o historico de tramitacoes.
# Gera hash do payload para deteccao de mudancas (CDC).
# ============================================================

# Informa o usuario que a ingestao de tramitacoes esta iniciando
print("Ingerindo tramitacoes (CDC)...")

# Lista para acumular todas as tramitacoes
tramitacoes_lista = []

# Total de proposicoes para calculo de progresso
total = len(proposicoes_lista)

# Loop: percorre cada proposicao para buscar tramitacoes
for i, prop in enumerate(proposicoes_lista):
    # Extrai o ID da proposicao atual
    prop_id = prop['id']
    
    try:
        # Busca tramitacoes desta proposicao
        dados = fetch_api(f"/proposicoes/{prop_id}/tramitacoes")
        
        # Para cada tramitacao, adiciona campos auxiliares e hash
        for d in dados:
            # Adiciona ID da proposicao como referencia
            d['_proposicao_id'] = prop_id
            # Adiciona tipo da proposicao (PL, PEC, etc)
            d['_sigla_tipo'] = prop.get('siglaTipo', '')
            # Adiciona numero da proposicao
            d['_numero'] = prop.get('numero', '')
            # Adiciona ano da proposicao
            d['_ano'] = prop.get('ano', '')
            # Gera hash MD5 do payload para CDC (deteccao de mudancas)
            payload = json.dumps(d, sort_keys=True, default=str)
            d['_payload_hash'] = str(hash(payload))
        
        # Adiciona as tramitacoes desta proposicao a lista geral
        tramitacoes_lista.extend(dados)
        
    # Erro de conexao (rede indisponivel)
    except requests.exceptions.ConnectionError:
        # Informa o usuario e interrompe
        print(f"  ERRO DE CONEXAO na proposicao {prop_id} - abortando")
        break
        
    # Qualquer outro erro
    except Exception as e:
        # Informa o usuario e continua
        print(f"  Erro na proposicao {prop_id}: {str(e)[:60]}")
    
    # A cada 100 proposicoes, exibe progresso
    if (i + 1) % 100 == 0:
        print(f"   Progresso: {i+1}/{total} ({(i+1)*100//total}%)")
        time.sleep(0.3)

# Exibe total de tramitacoes obtidas
print(f"   Total tramitacoes: {len(tramitacoes_lista)} registros")

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
# GRAVACAO NA CAMADA BRONZE
# ============================================================
# Grava proposicoes e tramitacoes em modo append.
# ============================================================

# Grava proposicoes na tabela bronze (append)
n1 = save_to_bronze(proposicoes_lista, "proposicoes", "/proposicoes", mode="append")

# Registra status para o resumo final
status_list.append({"tabela": "ft_bronze.proposicoes", "registros": n1})

# Grava tramitacoes com hash CDC na tabela bronze (append)
n2 = save_to_bronze(tramitacoes_lista, "tramitacoes", "/proposicoes/{id}/tramitacoes", mode="append")

# Registra status para o resumo final
status_list.append({"tabela": "ft_bronze.tramitacoes", "registros": n2})

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
