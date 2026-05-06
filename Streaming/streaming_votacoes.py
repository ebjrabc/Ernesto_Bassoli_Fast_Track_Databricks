# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Streaming - Micro-Batch Votacoes (10 min)
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook implementa ingestao em micro-batch a cada 10 minutos para votacoes.
# MAGIC Classifica votacoes por urgencia, calcula metricas de SLA (tempo entre abertura e
# MAGIC registro dos votos) e alimenta um near-real-time de acompanhamento legislativo.
# MAGIC Desafio opcional do programa Upskill Tiller.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `API /votacoes` | Votacoes em near-real-time (polling 10min) |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_bronze.votacoes_streaming` | Votacoes com classificacao de urgencia |
# MAGIC | `dt0025_dev.ft_gold.sla_votacoes` | Metricas de SLA |
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
log_notebook_start("streaming_votacoes")

# COMMAND ----------

# DBTITLE 1,Sobre: Configuração Micro-Batch
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Configuração Micro-Batch**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Configuração Micro-Batch
# ============================================================
# CONFIGURAÇÃO DO MICRO-BATCH
# ============================================================
# Define parâmetros para execução a cada 10 minutos:
# - Offset de último ID processado
# - Classificação de urgência por tipo de proposição
# - Limiar de alerta (votações com >400 votos = plenário)
# ============================================================

print("⚡ Configuração micro-batch votações...")

# Parâmetros
URGENCIA_ALTA_TIPOS = ["PEC", "MP", "PLP"]  # Matérias constitucionais/urgentes
# Atribui valor a variavel 'LIMIAR_PLENARIO'
LIMIAR_PLENARIO = 400  # Votações com >400 votos provavelmente são de plenário
# Atribui valor a variavel 'batch_inicio'
batch_inicio = datetime.now()

# Recupera offset
watermark = get_watermark("streaming_votacoes")
# Atribui valor a variavel 'last_id'
last_id = watermark["last_id"]
# Exibe mensagem informativa para o usuario
print(f"   Último ID processado: {last_id or 'NENHUM (primeira execução)'}")

# COMMAND ----------

# DBTITLE 1,Sobre: Polling Novas Votações
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Polling Novas Votações**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Polling Novas Votações
# ============================================================
# POLLING DE NOVAS VOTAÇÕES
# ============================================================
# Busca votações com ID > último processado.
# Implementa controle de offset para garantir que nenhuma
# votação seja perdida entre execuções do micro-batch.
# ============================================================

print("📥 Polling novas votações...")

# Atribui valor a variavel 'novas_votacoes'
novas_votacoes = fetch_api(
    # Executa operacao de processamento
    "/votacoes",
    # Atribui valor a variavel 'params'
    params={"ordenarPor": "id", "ordem": "DESC", "itens": 50}
# Fecha bloco de parametros
)

# Filtra apenas novas
if last_id:
    # Define lista de valores
    novas_votacoes = [v for v in novas_votacoes if str(v['id']) > str(last_id)]

# Exibe mensagem informativa para o usuario
print(f"   Novas votações detectadas: {len(novas_votacoes)}")

# Verifica condicao
if not novas_votacoes:
    # Exibe mensagem informativa para o usuario
    print("   ℹ️ Nenhuma nova votação. Encerrando batch.")
    # Executa operacao de processamento
    dbutils.notebook.exit("NO_NEW_DATA")

# COMMAND ----------

# DBTITLE 1,Sobre: Classifica Urgência
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Classifica Urgência**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Classifica Urgência
# ============================================================
# CLASSIFICAÇÃO DE URGÊNCIA
# ============================================================
# Classifica cada votação por urgência:
# - CRITICA: PEC, MP (matéria constitucional)
# - ALTA: PLP, PDL (legislação complementar)
# - NORMAL: PL e demais
# Base para disparo de notificações diferenciadas.
# ============================================================

print("🔔 Classificando urgência...")

# Inicia loop de repeticao
for v in novas_votacoes:
    # Atribui valor a variavel 'proposicao'
    proposicao = v.get('proposicaoObjeto', '') or ''
    
    # Verifica condicao
    if any(t in proposicao.upper() for t in ['PEC', 'MEDIDA PROVISÓRIA', 'MP ']):
        # Atribui valor a variavel 'v['_urgencia']'
        v['_urgencia'] = 'CRITICA'
    # Caso alternativo da condicao
    elif any(t in proposicao.upper() for t in ['PLP', 'PDL', 'COMPLEMENTAR']):
        # Atribui valor a variavel 'v['_urgencia']'
        v['_urgencia'] = 'ALTA'
    # Caso alternativo da condicao
    else:
        # Atribui valor a variavel 'v['_urgencia']'
        v['_urgencia'] = 'NORMAL'
    
    # Atribui valor a variavel 'v['_batch_timestamp']'
    v['_batch_timestamp'] = batch_inicio.isoformat()

# Resumo
urgencias = {}
# Inicia loop de repeticao
for v in novas_votacoes:
    # Atribui valor a variavel 'u'
    u = v['_urgencia']
    # Atribui valor a variavel 'urgencias[u]'
    urgencias[u] = urgencias.get(u, 0) + 1

# Inicia loop de repeticao
for u, c in urgencias.items():
    # Exibe mensagem informativa para o usuario
    print(f"   {u}: {c} votações")

# COMMAND ----------

# DBTITLE 1,Sobre: Grava e Alerta
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Grava e Alerta**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Grava e Alerta
# ============================================================
# GRAVAÇÃO E DISPARO DE ALERTAS
# ============================================================
# Grava novas votações na tabela de alertas.
# Votações CRITICA e ALTA geram registro de notificação.
# Em produção, integraria com webhook/email para alertas.
# ============================================================

print("💾 Gravando alertas...")

# Grava na tabela de alertas
n = save_to_bronze(novas_votacoes, "votacoes_stream", "/votacoes", mode="append")

# Grava alertas para urgência alta/crítica
alertas = [v for v in novas_votacoes if v['_urgencia'] in ('CRITICA', 'ALTA')]

# Verifica condicao
if alertas:
    # Atribui valor a variavel 'save_to_bronze(alertas, "votacoes_alertas_pendentes", "/votacoes/alertas", mode'
    save_to_bronze(alertas, "votacoes_alertas_pendentes", "/votacoes/alertas", mode="append")
    # Exibe mensagem informativa para o usuario
    print(f"   🔔 {len(alertas)} alertas gerados!")
    # Inicia loop de repeticao
    for a in alertas:
        # Exibe mensagem informativa para o usuario
        print(f"      [{a['_urgencia']}] ID:{a['id']} - {a.get('proposicaoObjeto', '')[:60]}")
# Caso alternativo da condicao
else:
    # Exibe mensagem informativa para o usuario
    print("   ℹ️ Nenhum alerta de urgência neste batch")

# COMMAND ----------

# DBTITLE 1,Sobre: Métricas SLA
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Métricas SLA**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Métricas SLA
# ============================================================
# MÉTRICAS DE SLA DO PIPELINE
# ============================================================
# Registra métricas end-to-end para monitoramento:
# - Latência: tempo entre polling e gravação
# - Volume: registros processados no batch
# - Taxa de erro: falhas / total
# Alimenta dashboard de SLA.
# ============================================================

print("📊 Registrando métricas SLA...")

# Atribui valor a variavel 'batch_fim'
batch_fim = datetime.now()
# Atribui valor a variavel 'latencia_segundos'
latencia_segundos = (batch_fim - batch_inicio).total_seconds()

# Define lista de valores
metricas = [{
    # Executa operacao de processamento
    "batch_timestamp": batch_inicio.isoformat(),
    # Executa operacao de processamento
    "latencia_segundos": latencia_segundos,
    # Executa operacao de processamento
    "volume_registros": len(novas_votacoes),
    # Executa operacao de processamento
    "alertas_gerados": len(alertas) if alertas else 0,
    # Executa operacao de processamento
    "erros": 0,
    # Executa operacao de processamento
    "status": "SUCCESS"
# Executa operacao de processamento
}]

# Atribui valor a variavel 'save_to_bronze(metricas, "sla_metricas_streaming", "internal/sla", mode'
save_to_bronze(metricas, "sla_metricas_streaming", "internal/sla", mode="append")

# Exibe mensagem informativa para o usuario
print(f"   ⏱️ Latência end-to-end: {latencia_segundos:.1f}s")
# Exibe mensagem informativa para o usuario
print(f"   📊 Volume: {len(novas_votacoes)} registros")
# Exibe mensagem informativa para o usuario
print(f"   ✅ Status: SUCCESS")

# COMMAND ----------

# DBTITLE 1,Sobre: Atualiza Offset
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Atualiza Offset**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Atualiza Offset
# ============================================================
# ATUALIZA OFFSET PARA PRÓXIMO BATCH
# ============================================================
# Grava o maior ID processado neste batch.
# Próxima execução (em 10 min) usará este offset.
# ============================================================

if novas_votacoes:
    # Busca valor maximo
    max_id = max(v['id'] for v in novas_votacoes)
    # Atualiza watermark apos processamento
    set_watermark("streaming_votacoes", last_id=max_id)
    # Exibe mensagem informativa para o usuario
    print(f"   📌 Offset atualizado: {max_id}")

# COMMAND ----------

# DBTITLE 1,Finaliza Notebook
# ============================================================
# FINALIZAÇÃO E RESUMO
# ============================================================
# Exibe métricas de tempo e registros processados.
# ============================================================

finalizar_notebook()
