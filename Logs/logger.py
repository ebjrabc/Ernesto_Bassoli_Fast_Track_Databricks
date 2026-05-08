# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

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
# MAGIC | `dt0025_dev.ft_bronze._pipeline_logs` | Tabela Delta com todos os logs |
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
# IMPORTS E CONFIGURACAO DO LOGGER
# ============================================================
# Configura tanto logging em console (print) quanto
# persistencia em tabela Delta para auditoria.
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

# Configuracao do formato de saida no console
logging.basicConfig(
    # Atribui valor a variavel 'level'
    level=logging.INFO,
    # Atribui valor a variavel 'format'
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    # Atribui valor a variavel 'datefmt'
    datefmt="%Y-%m-%d %H:%M:%S"
# Fecha bloco de parametros
)

# Cria instancia do logger com nome do projeto
logger = logging.getLogger("fast_track")

# ============================================================
# CONSTANTES DA TABELA DE LOGS
# ============================================================

# Catalogo no Unity Catalog
LOG_CATALOG = "uc_fast_track"

# Schema onde fica a tabela de logs
LOG_SCHEMA = "ft_bronze"

# Nome da tabela de logs
LOG_TABLE = "_pipeline_logs"

# Nome completo (catalog.schema.table)
LOG_FULL_TABLE = f"{LOG_CATALOG}.{LOG_SCHEMA}.{LOG_TABLE}"

# COMMAND ----------

# DBTITLE 1,Cria o catalogo se não existir
spark.sql(f"""CREATE CATALOG IF NOT EXISTS {LOG_CATALOG}""")

# COMMAND ----------

# DBTITLE 1,Cria o schema log se não existir
spark.sql(f"""CREATE DATABASE IF NOT EXISTS {LOG_CATALOG}.{LOG_SCHEMA}""")

# COMMAND ----------

# DBTITLE 1,Criacao da Tabela
# MAGIC %md
# MAGIC ## Criacao da Tabela de Logs
# MAGIC
# MAGIC Na celula abaixo e criada a tabela Delta para persistir os logs (se nao existir).

# COMMAND ----------

# DBTITLE 1,Cria Tabela Logs
# ============================================================
# CRIACAO DA TABELA DE LOGS
# ============================================================
# Cria a tabela Delta se nao existir. Estrutura:
# log_id, timestamp, nivel, notebook, etapa, mensagem,
# detalhes, duracao_segundos, registros_afetados,
# status, erro_tipo, erro_stack.
# ============================================================

# Comando SQL para criar a tabela de logs
spark.sql(f"""
    -- Executa operacao de processamento
    CREATE TABLE IF NOT EXISTS {LOG_FULL_TABLE} (
        -- Executa operacao de processamento
        log_id STRING,
        -- Executa operacao de processamento
        timestamp TIMESTAMP,
        -- Executa operacao de processamento
        nivel STRING,
        -- Executa operacao de processamento
        notebook STRING,
        -- Executa operacao de processamento
        etapa STRING,
        -- Executa operacao de processamento
        mensagem STRING,
        -- Executa operacao de processamento
        detalhes STRING,
        -- Executa operacao de processamento
        duracao_segundos DOUBLE,
        -- Executa operacao de processamento
        registros_afetados LONG,
        -- Executa operacao de processamento
        status STRING,
        -- Executa operacao de processamento
        erro_tipo STRING,
        -- Executa operacao de processamento
        erro_stack STRING
    -- Fecha bloco de parametros
    )
    -- Executa operacao de processamento
    USING DELTA
    -- Executa operacao de processamento
    COMMENT 'Logs centralizados do pipeline Fast Track - Camara dos Deputados'
-- Executa operacao de processamento
""")

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
# FUNCOES DE LOGGING
# ============================================================
# Funcoes para registrar eventos em diferentes niveis.
# Cada funcao grava no console E na tabela Delta.
# ============================================================

# Variavel global para rastrear notebook atual
_CURRENT_NOTEBOOK = "UNKNOWN"

# Variavel global para timestamp de inicio do notebook
_NOTEBOOK_START_TIME = None


# Define a funcao '_generate_log_id'
def _generate_log_id():
    # Executa operacao de processamento
    """Gera ID unico para cada registro de log"""
    # Usa UUID4 truncado para 8 caracteres
    return str(uuid.uuid4())[:8]


# Define a funcao '_persist_log'
def _persist_log(nivel, etapa, mensagem, detalhes="", duracao=None, registros=None, erro_tipo="", erro_stack=""):
    # Executa operacao de processamento
    """Persiste log na tabela Delta (funcao interna)"""
    # Executa operacao de processamento
    global _CURRENT_NOTEBOOK
    # Inicia bloco de tratamento de erros
    try:
        # Monta dicionario com todos os campos do log
        data = [{
            # Executa operacao de processamento
            "log_id": _generate_log_id(),
            # Executa operacao de processamento
            "timestamp": datetime.now(),
            # Executa operacao de processamento
            "nivel": nivel,
            # Executa operacao de processamento
            "notebook": _CURRENT_NOTEBOOK,
            # Executa operacao de processamento
            "etapa": etapa,
            # Executa operacao de processamento
            "mensagem": mensagem,
            # Executa operacao de processamento
            "detalhes": str(detalhes)[:2000] if detalhes else "",
            # Executa operacao de processamento
            "duracao_segundos": duracao,
            # Executa operacao de processamento
            "registros_afetados": registros,
            # Executa operacao de processamento
            "status": "OK" if nivel not in ("ERROR", "CRITICAL") else "FALHA",
            # Executa operacao de processamento
            "erro_tipo": erro_tipo,
            # Executa operacao de processamento
            "erro_stack": str(erro_stack)[:2000] if erro_stack else ""
        # Executa operacao de processamento
        }]
        
        # CORRECAO: Define schema explicito para evitar erro de inferencia
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
        
        log_schema = StructType([
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
        
        # Converte para DataFrame com schema explicito
        df = spark.createDataFrame(data, schema=log_schema)
        # Executa operacao de processamento
        df.write.format("delta").mode("append").saveAsTable(LOG_FULL_TABLE)
    # Captura e trata o erro
    except Exception as e:
        # Se falhar ao gravar log, apenas avisa (nao interrompe pipeline)
        logger.warning(f"Falha ao persistir log: {str(e)[:100]}")


# Define a funcao 'log_info'
def log_info(etapa, mensagem, detalhes="", registros=None):
    # Executa operacao de processamento
    """Registra informacao de operacao normal"""
    # Exibe no console
    logger.info(f"[{etapa}] {mensagem}")
    # Persiste na tabela Delta
    _persist_log("INFO", etapa, mensagem, detalhes, registros=registros)


# Define a funcao 'log_warn'
def log_warn(etapa, mensagem, detalhes=""):
    # Executa operacao de processamento
    """Registra aviso de situacao inesperada"""
    # Executa operacao de processamento
    logger.warning(f"[{etapa}] {mensagem}")
    # Executa operacao de processamento
    _persist_log("WARN", etapa, mensagem, detalhes)


# Define a funcao 'log_error'
def log_error(etapa, mensagem, exception=None, detalhes=""):
    # Executa operacao de processamento
    """Registra erro recuperavel com stack trace"""
    # Extrai tipo e stack trace da excecao
    erro_tipo = type(exception).__name__ if exception else ""
    # Atribui valor a variavel 'erro_stack'
    erro_stack = traceback.format_exc() if exception else ""
    # Executa operacao de processamento
    logger.error(f"[{etapa}] {mensagem} | {erro_tipo}")
    # Atribui valor a variavel '_persist_log("ERROR", etapa, mensagem, detalhes, erro_tipo'
    _persist_log("ERROR", etapa, mensagem, detalhes, erro_tipo=erro_tipo, erro_stack=erro_stack)


# Define a funcao 'log_critical'
def log_critical(etapa, mensagem, exception=None):
    # Executa operacao de processamento
    """Registra erro critico que interrompe pipeline"""
    # Atribui valor a variavel 'erro_tipo'
    erro_tipo = type(exception).__name__ if exception else ""
    # Atribui valor a variavel 'erro_stack'
    erro_stack = traceback.format_exc() if exception else ""
    # Executa operacao de processamento
    logger.critical(f"[{etapa}] {mensagem} | {erro_tipo}")
    # Atribui valor a variavel '_persist_log("CRITICAL", etapa, mensagem, erro_tipo'
    _persist_log("CRITICAL", etapa, mensagem, erro_tipo=erro_tipo, erro_stack=erro_stack)


# Define a funcao 'log_success'
def log_success(etapa, mensagem, duracao=None, registros=None):
    # Executa operacao de processamento
    """Registra conclusao bem-sucedida"""
    # Executa operacao de processamento
    logger.info(f"[{etapa}] {mensagem}")
    # Atribui valor a variavel '_persist_log("SUCCESS", etapa, mensagem, duracao'
    _persist_log("SUCCESS", etapa, mensagem, duracao=duracao, registros=registros)


# Define a funcao 'log_api_call'
def log_api_call(endpoint, status_code, registros=None, duracao=None, erro=None):
    # Executa operacao de processamento
    """Registra chamada a API com resultado"""
    # Verifica condicao
    if status_code == 200:
        # Atribui valor a variavel 'log_info("API_CALL", f"GET {endpoint} -> 200 OK", registros'
        log_info("API_CALL", f"GET {endpoint} -> 200 OK", registros=registros)
    # Caso alternativo da condicao
    elif status_code == 429:
        # Executa operacao de processamento
        log_warn("API_RATE_LIMIT", f"GET {endpoint} -> 429 Rate Limited")
    # Caso alternativo da condicao
    elif status_code == 404:
        # Executa operacao de processamento
        log_warn("API_NOT_FOUND", f"GET {endpoint} -> 404 Nao encontrado")
    # Caso alternativo da condicao
    elif status_code >= 500:
        # Atribui valor a variavel 'log_error("API_SERVER_ERROR", f"GET {endpoint} -> {status_code}", detalhes'
        log_error("API_SERVER_ERROR", f"GET {endpoint} -> {status_code}", detalhes=str(erro))
    # Caso alternativo da condicao
    else:
        # Atribui valor a variavel 'log_warn("API_UNEXPECTED", f"GET {endpoint} -> {status_code}", detalhes'
        log_warn("API_UNEXPECTED", f"GET {endpoint} -> {status_code}", detalhes=str(erro))


# Define a funcao 'log_api_connection_error'
def log_api_connection_error(endpoint, exception):
    # Executa operacao de processamento
    """Registra falha de conexao com a API com mensagem detalhada"""
    # Registra no log persistente
    log_error("API_CONNECTION_FAILED", f"FALHA DE CONEXAO: {endpoint}", exception=exception,
        # Atribui valor a variavel 'detalhes'
        detalhes=f"Possiveis causas: API fora do ar, problema de rede, timeout de conexao")
    # Exibe mensagem detalhada para o usuario
    print(f"\n{'='*60}")
    # Exibe mensagem informativa para o usuario
    print(f"  ERRO DE CONEXAO COM A API")
    # Exibe mensagem informativa para o usuario
    print(f"{'='*60}")
    # Exibe mensagem informativa para o usuario
    print(f"  Endpoint: {endpoint}")
    # Exibe mensagem informativa para o usuario
    print(f"  Erro: {type(exception).__name__}: {str(exception)[:200]}")
    # Exibe mensagem informativa para o usuario
    print(f"\n  Possiveis causas:")
    # Exibe mensagem informativa para o usuario
    print(f"    1. API da Camara fora do ar")
    # Exibe mensagem informativa para o usuario
    print(f"    2. Problema de rede/firewall no cluster")
    # Exibe mensagem informativa para o usuario
    print(f"    3. Timeout de conexao (servidor lento)")
    # Exibe mensagem informativa para o usuario
    print(f"    4. URL incorreta ou DNS nao resolvido")
    # Exibe mensagem informativa para o usuario
    print(f"\n  Acao recomendada:")
    # Exibe mensagem informativa para o usuario
    print(f"    -> Aguardar e tentar novamente em alguns minutos")
    # Exibe mensagem informativa para o usuario
    print(f"    -> Verificar: https://dadosabertos.camara.leg.br/api/v2")
    # Exibe mensagem informativa para o usuario
    print(f"{'='*60}\n")


# Define a funcao 'log_api_timeout'
def log_api_timeout(endpoint, timeout_seconds):
    # Executa operacao de processamento
    """Registra timeout na chamada API"""
    # Executa operacao de processamento
    log_warn("API_TIMEOUT", f"Timeout apos {timeout_seconds}s em {endpoint}")
    # Exibe mensagem informativa para o usuario
    print(f"  Timeout ({timeout_seconds}s) em {endpoint} - retentando...")


# Define a funcao 'log_notebook_start'
def log_notebook_start(notebook_name):
    # Executa operacao de processamento
    """Registra inicio da execucao de um notebook"""
    # Executa operacao de processamento
    global _CURRENT_NOTEBOOK, _NOTEBOOK_START_TIME
    # Define notebook atual no contexto global
    _CURRENT_NOTEBOOK = notebook_name
    # Registra timestamp de inicio
    _NOTEBOOK_START_TIME = datetime.now()
    # Persiste no log
    log_info("NOTEBOOK_START", f"Iniciando notebook: {notebook_name}")
    # Exibe no console
    print(f"\n{'='*60}")
    # Exibe mensagem informativa para o usuario
    print(f"  INICIO: {notebook_name}")
    # Exibe mensagem informativa para o usuario
    print(f"  Hora: {_NOTEBOOK_START_TIME.strftime('%Y-%m-%d %H:%M:%S')}")
    # Exibe mensagem informativa para o usuario
    print(f"{'='*60}")


# Define a funcao 'log_notebook_end'
def log_notebook_end(notebook_name, status="SUCCESS", erro=None):
    # Executa operacao de processamento
    """Registra fim da execucao de um notebook"""
    # Executa operacao de processamento
    global _NOTEBOOK_START_TIME
    # Calcula duracao
    duracao = None
    # Verifica condicao
    if _NOTEBOOK_START_TIME:
        # Atribui valor a variavel 'duracao'
        duracao = (datetime.now() - _NOTEBOOK_START_TIME).total_seconds()
    # Registra conforme status
    if status == "SUCCESS":
        # Atribui valor a variavel 'log_success("NOTEBOOK_END", f"Notebook finalizado: {notebook_name}", duracao'
        log_success("NOTEBOOK_END", f"Notebook finalizado: {notebook_name}", duracao=duracao)
        # Exibe mensagem informativa para o usuario
        print(f"\n{'='*60}")
        # Exibe mensagem informativa para o usuario
        print(f"  FIM: {notebook_name}")
        # Verifica condicao
        if duracao:
            # Exibe mensagem informativa para o usuario
            print(f"  Duracao: {duracao:.1f}s ({duracao/60:.2f} min)")
        # Exibe mensagem informativa para o usuario
        print(f"  Status: SUCESSO")
        # Exibe mensagem informativa para o usuario
        print(f"{'='*60}")
    # Caso alternativo da condicao
    else:
        # Atribui valor a variavel 'log_error("NOTEBOOK_FAILED", f"Notebook FALHOU: {notebook_name}", exception'
        log_error("NOTEBOOK_FAILED", f"Notebook FALHOU: {notebook_name}", exception=erro)
        # Exibe mensagem informativa para o usuario
        print(f"\n{'='*60}")
        # Exibe mensagem informativa para o usuario
        print(f"  FALHA: {notebook_name}")
        # Verifica condicao
        if duracao:
            # Exibe mensagem informativa para o usuario
            print(f"  Duracao: {duracao:.1f}s")
        # Verifica condicao
        if erro:
            # Exibe mensagem informativa para o usuario
            print(f"  Erro: {str(erro)[:200]}")
        # Exibe mensagem informativa para o usuario
        print(f"{'='*60}")


# Define a funcao 'log_table_write'
def log_table_write(table_name, registros, mode="overwrite"):
    # Executa operacao de processamento
    """Registra gravacao em tabela"""
    # Atribui valor a variavel 'log_success("TABLE_WRITE", f"Gravado: {table_name} ({registros} registros, mode'
    log_success("TABLE_WRITE", f"Gravado: {table_name} ({registros} registros, mode={mode})", registros=registros)


# Define a funcao 'log_quality_check'
def log_quality_check(table_name, total, issues):
    # Executa operacao de processamento
    """Registra resultado de validacao de qualidade"""
    # Verifica condicao
    if issues:
        # Atribui valor a variavel 'log_warn("QUALITY_CHECK", f"Qualidade [{table_name}]: {len(issues)} problemas", detalhes'
        log_warn("QUALITY_CHECK", f"Qualidade [{table_name}]: {len(issues)} problemas", detalhes=str(issues))
    # Caso alternativo da condicao
    else:
        # Executa operacao de processamento
        log_info("QUALITY_CHECK", f"Qualidade [{table_name}]: OK ({total} registros)")
