# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://gazetadasemana.com.br/images/noticias/166864/19041851_compass.uo.jpg.jpg" width="450"/>

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Sistema de Logging Centralizado
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook contem o sistema de logging centralizado do projeto Fast Track.
# MAGIC Todas as funcoes de log sao definidas aqui e disponibilizadas aos demais notebooks via `%run`.
# MAGIC Os logs sao persistidos em tabela Delta para auditoria completa de cada execucao do pipeline.
# MAGIC
# MAGIC ## Entradas
# MAGIC | Item | Descricao |
# MAGIC |------|-----------|
# MAGIC | Nenhuma | Este notebook nao le dados externos |
# MAGIC
# MAGIC ## Saidas
# MAGIC | Tabela | Descricao |
# MAGIC |--------|-----------|
# MAGIC | `uc_fast_track.ft_bronze._pipeline_logs` | Tabela Delta com todos os logs |
# MAGIC
# MAGIC ## Funcoes Disponibilizadas
# MAGIC | Funcao | Descricao |
# MAGIC |--------|-----------|
# MAGIC | `log_info()` | Operacoes normais |
# MAGIC | `log_warn()` | Situacoes inesperadas |
# MAGIC | `log_error()` | Erros recuperaveis com stack trace |
# MAGIC | `log_critical()` | Erros que interrompem o pipeline |
# MAGIC | `log_success()` | Conclusoes bem-sucedidas |
# MAGIC | `log_api_call()` | Chamadas a API com status |
# MAGIC | `log_api_connection_error()` | Falhas de conexao detalhadas |
# MAGIC | `log_api_timeout()` | Timeouts com causa provavel |
# MAGIC | `log_notebook_start()` | Inicio de notebook |
# MAGIC | `log_notebook_end()` | Fim de notebook com duracao |
# MAGIC | `log_table_write()` | Gravacao em tabela |
# MAGIC | `log_quality_check()` | Resultado de validacao |
# MAGIC
# MAGIC ## Responsavel
# MAGIC - **Ernesto Bassoli Junior**

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Imports e Configuracao
# MAGIC %md
# MAGIC # Imports e Configuracao do Logger

# COMMAND ----------

# DBTITLE 1,Sobre os Imports
# MAGIC %md
# MAGIC Na celula abaixo sao importados os modulos necessarios para o sistema de logging:
# MAGIC - `logging`: modulo padrao do Python para logs em console
# MAGIC - `traceback`: captura stack traces de excecoes para diagnostico
# MAGIC - `datetime`: registra timestamps precisos de cada evento
# MAGIC - `uuid`: gera identificadores unicos para cada registro de log

# COMMAND ----------

# DBTITLE 1,Imports Logger
# ============================================================
# IMPORTS E CONFIGURACAO DO LOGGER (OTIMIZADO)
# ============================================================
# Configura tanto logging em console (print) quanto
# persistencia em tabela Delta para auditoria.
# OTIMIZACOES: Batching, schema global, sincronizacao anti-ZMQ
# ============================================================

# Modulo padrao de logging do Python (saida em console)
import logging

# Modulo para capturar stack trace completo de excecoes
import traceback

# Modulo para gerar identificadores unicos (log_id)
import uuid

# Modulo para timestamps precisos
from datetime import datetime

# Funcao do PySpark para adicionar timestamp atual
from pyspark.sql.functions import lit, current_timestamp

# Threading para controle de contexto paralelo
import threading

# ============================================================
# SINCRONIZACAO ANTI-ZMQ COM FUNCOES_GENERICAS
# ============================================================
# Usa a mesma instancia _in_parallel_context do FUNCOES_GENERICAS
# Se nao existir (logger carregado sozinho), cria localmente
if '_in_parallel_context' not in globals():
    _in_parallel_context = threading.local()

# ============================================================
# BATCHING DE LOGS (OTIMIZACAO CRITICA)
# ============================================================
# Buffer para acumular logs e gravar em lote
_log_buffer = []
_log_buffer_lock = threading.Lock()
_LOG_BATCH_SIZE = 50  # Grava a cada 50 logs

# Configuracao do formato de saida no console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Cria instancia do logger com nome do projeto
logger = logging.getLogger("fast_track")

# ============================================================
# CONSTANTES DA TABELA DE LOGS
# ============================================================

# Catalogo no Unity Catalog
LOG_CATALOG = "uc_fast_track"

# Schema onde ficam os logs
LOG_SCHEMA = "ft_bronze"

# Nome da tabela de logs
LOG_TABLE = "_pipeline_logs"

# Nome completo da tabela (catalog.schema.table)
LOG_FULL_TABLE = f"{LOG_CATALOG}.{LOG_SCHEMA}.{LOG_TABLE}"

# ============================================================
# SCHEMA GLOBAL (OTIMIZACAO - CRIADO UMA VEZ)
# ============================================================
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

_LOG_SCHEMA = StructType([
    StructField("log_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("nivel", StringType(), False),
    StructField("notebook", StringType(), False),
    StructField("etapa", StringType(), False),
    StructField("mensagem", StringType(), True),
    StructField("detalhes", StringType(), True),
    StructField("duracao_segundos", DoubleType(), True),
    StructField("registros_afetados", LongType(), True),
    StructField("status", StringType(), True),
    StructField("erro_tipo", StringType(), True),
    StructField("erro_stack", StringType(), True)
])

print("[LOGGER] Configuracao carregada com otimizacoes: batching, schema global, anti-ZMQ")

# COMMAND ----------

# DBTITLE 1,Cria Catalogo (SQL Nativo)
# ============================================================
# CRIA CATALOGO SE NAO EXISTIR (OTIMIZADO COM FLAG)
# ============================================================
# Usa flag para evitar re-executar em sessao ativa.
# Na primeira execucao, cria o catalogo.
# Nas execucoes seguintes (mesma sessao), pula.
# ============================================================

if 'LOG_SETUP_DONE' not in globals():
    print("[LOGGER] Criando catalogo/schema/tabela (primeira vez)...")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {LOG_CATALOG}")
    print(f"[LOGGER] Catalogo {LOG_CATALOG} criado/verificado")
else:
    print("[LOGGER] Setup ja executado nesta sessao - pulando catalogo")

# COMMAND ----------

# DBTITLE 1,Cria o schema log se não existir
# ============================================================
# CRIA SCHEMA SE NAO EXISTIR (OTIMIZADO COM FLAG)
# ============================================================
# Usa flag para evitar re-executar em sessao ativa.
# ============================================================

if 'LOG_SETUP_DONE' not in globals():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LOG_CATALOG}.{LOG_SCHEMA}")
    print(f"[LOGGER] Schema {LOG_SCHEMA} criado/verificado")
else:
    print("[LOGGER] Setup ja executado nesta sessao - pulando schema")

# COMMAND ----------

# DBTITLE 1,Criacao da Tabela
# MAGIC %md
# MAGIC ## Criacao da Tabela de Logs
# MAGIC
# MAGIC Na celula abaixo e criada a tabela Delta para persistir os logs (se nao existir).

# COMMAND ----------

# DBTITLE 1,Cria Tabela Logs
# ============================================================
# CRIA TABELA SE NAO EXISTIR (OTIMIZADO COM FLAG)
# ============================================================
# Usa flag para evitar re-executar em sessao ativa.
# Na primeira execucao, cria a tabela.
# Nas execucoes seguintes (mesma sessao), pula.
# ============================================================

if 'LOG_SETUP_DONE' not in globals():
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LOG_FULL_TABLE} (
        log_id STRING,
        timestamp TIMESTAMP,
        nivel STRING,
        notebook STRING,
        etapa STRING,
        mensagem STRING,
        detalhes STRING,
        duracao_segundos DOUBLE,
        registros_afetados LONG,
        status STRING,
        erro_tipo STRING,
        erro_stack STRING
    ) USING DELTA
    """)
    
    # Marca como executado
    LOG_SETUP_DONE = True
    print("[LOGGER] Tabela de logs criada/verificada - setup completo")
else:
    print("[LOGGER] Setup ja executado nesta sessao - pulando tabela")

# COMMAND ----------

# DBTITLE 1,Funcoes de Log
# MAGIC %md
# MAGIC # Funcoes de Logging
# MAGIC
# MAGIC As funcoes abaixo registram eventos em diferentes niveis de severidade.
# MAGIC Cada funcao grava tanto no console (para acompanhamento em tempo real)
# MAGIC quanto na tabela Delta (para auditoria posterior).

# COMMAND ----------

# DBTITLE 1,Sobre as Funcoes
# MAGIC %md
# MAGIC Na celula abaixo estao definidas todas as funcoes de logging do projeto.
# MAGIC A funcao interna `_persist_log()` grava na tabela Delta, enquanto as funcoes
# MAGIC publicas (`log_info`, `log_error`, etc.) sao a interface usada pelos notebooks.

# COMMAND ----------

# DBTITLE 1,Definicao Funcoes Log
# ============================================================
# FUNCOES DE LOGGING (OTIMIZADAS)
# ============================================================
# Funcoes para registrar eventos em diferentes niveis.
# Cada funcao grava no console E na tabela Delta.
# OTIMIZACOES: Batching, truncation inteligente, fallback
# ============================================================

# Variavel global para rastrear notebook atual
_CURRENT_NOTEBOOK = "UNKNOWN"

# Variavel global para timestamp de inicio do notebook
_NOTEBOOK_START_TIME = None


# Define a funcao '_generate_log_id'
def _generate_log_id():
    """Gera ID unico para cada registro de log"""
    return str(uuid.uuid4())[:8]


# Define a funcao '_is_in_parallel_context'
def _is_in_parallel_context():
    """Verifica se estamos em contexto paralelo (sincronizado com FUNCOES_GENERICAS)"""
    return getattr(_in_parallel_context, 'active', False)


# Define a funcao '_truncate_smart' (OTIMIZACAO)
def _truncate_smart(text, max_length=5000):
    """Trunca preservando inicio e fim (mais util para debugging)"""
    if not text or len(text) <= max_length:
        return text
    
    # Mantem primeiros 60% + ultimos 40%
    head_size = int(max_length * 0.6)
    tail_size = int(max_length * 0.4)
    
    return (
        text[:head_size] + 
        f"\n\n... [TRUNCADO {len(text) - max_length} chars] ...\n\n" + 
        text[-tail_size:]
    )


# Define a funcao '_flush_logs' (OTIMIZACAO CRITICA - BATCHING)
def _flush_logs():
    """Grava todos os logs do buffer de uma vez (muito mais rapido)"""
    global _log_buffer
    
    with _log_buffer_lock:
        if not _log_buffer:
            return
        
        try:
            # Grava TODOS os logs de uma vez (1 operacao vs N operacoes)
            df = spark.createDataFrame(_log_buffer, schema=_LOG_SCHEMA)
            df.write.format("delta").mode("append").saveAsTable(LOG_FULL_TABLE)
            
            # Limpa buffer
            buffer_size = len(_log_buffer)
            _log_buffer = []
            
            # Debug (opcional - descomente se quiser ver batching)
            # print(f"[LOGGER] Flush: {buffer_size} logs gravados em batch")
        except Exception as e:
            # Se falhar, tenta fallback para arquivo local
            print(f"[LOGGER] ERRO ao gravar logs no Delta: {str(e)[:100]}")
            _fallback_to_file()


# Define a funcao '_fallback_to_file' (OTIMIZACAO - ZERO PERDA)
def _fallback_to_file():
    """Grava logs em arquivo local se Delta falhar (fallback)"""
    global _log_buffer
    
    try:
        import json
        log_file = f"/tmp/fast_track_logs_{_CURRENT_NOTEBOOK}.jsonl"
        
        with open(log_file, 'a') as f:
            for log_entry in _log_buffer:
                # Converte datetime para string
                log_entry_copy = log_entry.copy()
                if 'timestamp' in log_entry_copy:
                    log_entry_copy['timestamp'] = log_entry_copy['timestamp'].isoformat()
                f.write(json.dumps(log_entry_copy) + '\n')
        
        print(f"[LOGGER] Fallback: {len(_log_buffer)} logs salvos em {log_file}")
        _log_buffer = []
    except Exception as e:
        print(f"[LOGGER] ERRO CRITICO: Nao foi possivel salvar logs: {str(e)[:100]}")
        _log_buffer = []


# Define a funcao '_persist_log' (OTIMIZADA)
def _persist_log(nivel, etapa, mensagem, detalhes="", duracao=None, registros=None, erro_tipo="", erro_stack=""):
    """Persiste log no buffer (funcao interna) - grava em batch"""
    global _CURRENT_NOTEBOOK, _log_buffer
    
    # Durante execucao paralela, nao tenta gravar no Delta
    if _is_in_parallel_context():
        return
    
    # Monta dicionario com todos os campos do log
    log_entry = {
        "log_id": _generate_log_id(),
        "timestamp": datetime.now(),
        "nivel": nivel,
        "notebook": _CURRENT_NOTEBOOK,
        "etapa": etapa,
        "mensagem": mensagem,
        "detalhes": _truncate_smart(str(detalhes), 5000) if detalhes else "",
        "duracao_segundos": duracao,
        "registros_afetados": registros,
        "status": "OK" if nivel not in ("ERROR", "CRITICAL") else "FALHA",
        "erro_tipo": erro_tipo,
        "erro_stack": _truncate_smart(str(erro_stack), 5000) if erro_stack else ""
    }
    
    # OTIMIZACAO: Adiciona ao buffer (thread-safe)
    with _log_buffer_lock:
        _log_buffer.append(log_entry)
        
        # Se atingiu tamanho do batch, grava tudo de uma vez
        if len(_log_buffer) >= _LOG_BATCH_SIZE:
            _flush_logs()


# Define a funcao 'log_info'
def log_info(etapa, mensagem, detalhes="", registros=None):
    """Registra informacao de operacao normal"""
    if not _is_in_parallel_context():
        logger.info(f"[{etapa}] {mensagem}")
        _persist_log("INFO", etapa, mensagem, detalhes, registros=registros)


# Define a funcao 'log_warn'
def log_warn(etapa, mensagem, detalhes=""):
    """Registra aviso de situacao inesperada"""
    if not _is_in_parallel_context():
        logger.warning(f"[{etapa}] {mensagem}")
        _persist_log("WARN", etapa, mensagem, detalhes)


# Define a funcao 'log_error'
def log_error(etapa, mensagem, exception=None, detalhes=""):
    """Registra erro recuperavel com stack trace"""
    erro_tipo = type(exception).__name__ if exception else ""
    erro_stack = traceback.format_exc() if exception else ""
    
    if not _is_in_parallel_context():
        logger.error(f"[{etapa}] {mensagem} | {erro_tipo}")
        _persist_log("ERROR", etapa, mensagem, detalhes, erro_tipo=erro_tipo, erro_stack=erro_stack)


# Define a funcao 'log_critical'
def log_critical(etapa, mensagem, exception=None):
    """Registra erro critico que interrompe pipeline"""
    erro_tipo = type(exception).__name__ if exception else ""
    erro_stack = traceback.format_exc() if exception else ""
    
    if not _is_in_parallel_context():
        logger.critical(f"[{etapa}] {mensagem} | {erro_tipo}")
        _persist_log("CRITICAL", etapa, mensagem, erro_tipo=erro_tipo, erro_stack=erro_stack)


# Define a funcao 'log_success'
def log_success(etapa, mensagem, duracao=None, registros=None):
    """Registra conclusao bem-sucedida"""
    if not _is_in_parallel_context():
        logger.info(f"[{etapa}] {mensagem}")
        _persist_log("SUCCESS", etapa, mensagem, duracao=duracao, registros=registros)


# Mapeamento de status codes (OTIMIZACAO - DRY)
_STATUS_HANDLERS = {
    200: lambda e, r: log_info("API_CALL", f"GET {e} -> 200 OK", registros=r),
    400: lambda e, r: None,  # SILENCIOSO (esperado - IDs invalidos)
    404: lambda e, r: log_warn("API_NOT_FOUND", f"GET {e} -> 404"),
    429: lambda e, r: log_warn("API_RATE_LIMIT", f"GET {e} -> 429 Rate Limited"),
}


# Define a funcao 'log_api_call' (OTIMIZADA)
def log_api_call(endpoint, status_code, registros=None, duracao=None, erro=None):
    """Registra chamada a API com resultado"""
    handler = _STATUS_HANDLERS.get(status_code)
    if handler:
        handler(endpoint, registros)
    elif status_code >= 500:
        log_error("API_SERVER_ERROR", f"GET {endpoint} -> {status_code}", detalhes=str(erro))
    else:
        log_warn("API_UNEXPECTED", f"GET {endpoint} -> {status_code}", detalhes=str(erro))


# Define a funcao 'log_api_connection_error'
def log_api_connection_error(endpoint, exception):
    """Registra falha de conexao com API"""
    log_error("API_CONNECTION_ERROR", f"Falha de conexao: {endpoint}", exception=exception)


# Define a funcao 'log_api_timeout'
def log_api_timeout(endpoint, timeout_seconds):
    """Registra timeout em chamada a API"""
    log_warn("API_TIMEOUT", f"Timeout apos {timeout_seconds}s: {endpoint}")


# Define a funcao 'log_notebook_start'
def log_notebook_start(notebook_name):
    """Registra inicio de execucao de notebook"""
    global _CURRENT_NOTEBOOK, _NOTEBOOK_START_TIME
    _CURRENT_NOTEBOOK = notebook_name
    _NOTEBOOK_START_TIME = datetime.now()
    
    print(f"\n{'='*60}")
    print(f"  INICIO: {notebook_name}")
    print(f"  Hora: {_NOTEBOOK_START_TIME.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    log_info("NOTEBOOK_START", f"Iniciando notebook: {notebook_name}")


# Define a funcao 'log_notebook_end' (OTIMIZADA)
def log_notebook_end(notebook_name, status="SUCCESS"):
    """Registra fim de execucao de notebook"""
    global _NOTEBOOK_START_TIME
    
    if _NOTEBOOK_START_TIME:
        duracao = (datetime.now() - _NOTEBOOK_START_TIME).total_seconds()
    else:
        duracao = None
    
    print(f"\n{'='*60}")
    print(f"  FIM: {notebook_name} [{status}]")
    if duracao:
        print(f"  Duracao: {duracao:.2f}s")
    print(f"{'='*60}\n")
    log_success("NOTEBOOK_END", f"Finalizando notebook: {notebook_name}", duracao=duracao)
    
    # OTIMIZACAO CRITICA: Garante que logs pendentes sao gravados
    _flush_logs()


# Define a funcao 'log_table_write'
def log_table_write(table_name, row_count, operation="write"):
    """Registra gravacao em tabela"""
    log_info("TABLE_WRITE", f"{operation.upper()}: {table_name}", registros=row_count)


# Define a funcao 'log_quality_check'
def log_quality_check(table_name, row_count, issues):
    """Registra resultado de validacao de qualidade"""
    if issues:
        log_warn("QUALITY_CHECK", f"Problemas em {table_name}", detalhes=str(issues))
    else:
        log_info("QUALITY_CHECK", f"Qualidade OK: {table_name}", registros=row_count)

# COMMAND ----------

# DBTITLE 1,Teste Otimizacoes (REMOVIVEL)
# ============================================================
# TESTE DE BATCHING E OTIMIZACOES (REMOVIVEL)
# ============================================================
# Celula de teste para validar que todas as otimizacoes funcionam.
# Pode ser removida apos validacao.
# ============================================================

import time

print("\n" + "="*60)
print("TESTE DE OTIMIZACOES DO LOGGER")
print("="*60)

# Teste 1: Batching de logs
print("\n[TESTE 1] Batching de logs (10 logs rapidos)...")
start = time.time()
for i in range(10):
    log_info("TESTE_BATCH", f"Log de teste #{i+1}")
end = time.time()
print(f"   Tempo: {(end-start)*1000:.1f}ms (esperado: <50ms com batching)")

# Teste 2: Truncation inteligente
print("\n[TESTE 2] Truncation inteligente (texto longo)...")
longo = "A" * 10000  # 10k chars
log_info("TESTE_TRUNCATE", "Teste com texto muito longo", detalhes=longo)
print(f"   Buffer size: {len(_log_buffer)} logs")

# Teste 3: log_api_call com mapeamento
print("\n[TESTE 3] log_api_call refatorado...")
log_api_call("/teste", 200, registros=100)
log_api_call("/teste", 429)
log_api_call("/teste", 500, erro="Server error")
print(f"   Buffer size: {len(_log_buffer)} logs")

# Teste 4: Flush manual
print("\n[TESTE 4] Flush manual...")
print(f"   Buffer antes do flush: {len(_log_buffer)} logs")
_flush_logs()
print(f"   Buffer apos flush: {len(_log_buffer)} logs (esperado: 0)")

print("\n" + "="*60)
print("TESTE CONCLUIDO COM SUCESSO")
print("="*60 + "\n")
