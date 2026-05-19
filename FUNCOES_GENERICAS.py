# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://gazetadasemana.com.br/images/noticias/166864/19041851_compass.uo.jpg.jpg" width="450"/>

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Notebook: FUNCOES_GENERICAS
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook contem todas as funcoes genericas reutilizaveis do projeto Fast Track.
# MAGIC Ele e executado via `%run` por todos os demais notebooks (Bronze, Silver, Gold e Streaming).
# MAGIC As funcoes aqui definidas encapsulam a logica de ingestao via API REST, gravacao em tabelas
# MAGIC Delta Lake, controle incremental por watermark, merge para camada Silver e validacoes de qualidade.
# MAGIC
# MAGIC ## Entradas (Dependencias)
# MAGIC | Item | Descricao |
# MAGIC |------|-----------|
# MAGIC | `Logs/logger.py` | Sistema de logging centralizado |
# MAGIC | API REST | `https://dadosabertos.camara.leg.br/api/v2` |
# MAGIC
# MAGIC ## Saidas (O que este notebook disponibiliza)
# MAGIC | Funcao | Descricao |
# MAGIC |--------|-----------|
# MAGIC | `fetch_api()` | Busca dados da API com paginacao, retry e logging |
# MAGIC | `fetch_sub_endpoint()` | Busca sub-recursos para lista de IDs |
# MAGIC | `save_to_bronze()` | Grava DataFrame na camada Bronze |
# MAGIC | `merge_to_silver()` | Faz MERGE incremental na camada Silver |
# MAGIC | `save_to_gold()` | Grava DataFrame na camada Gold |
# MAGIC | `get_watermark()` / `set_watermark()` | Controle de carga incremental |
# MAGIC | `check_quality()` | Validacao de qualidade dos dados |
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

# DBTITLE 1,Sobre o Logger
# MAGIC %md
# MAGIC Ao executar o comando `%run ./Logs/logger`, o Python ira interpretar e executar o conteudo
# MAGIC do notebook `logger.py`, disponibilizando todas as funcoes de logging (log_info, log_error, etc.)
# MAGIC para uso nas demais funcoes deste notebook.

# COMMAND ----------

# DBTITLE 1,Importacao do Logger (OPCIONAL)
# ============================================================
# LOGGER - FUNCOES DUMMY (FALLBACK)
# ============================================================
# IMPORTANTE: O logger e OPCIONAL. Cada notebook Bronze pode
# escolher se quer carregar o logger ou nao.
# 
# Para usar logging:
#   1. No notebook Bronze, adicione: %run ../Logs/logger
#   2. Isso sobrescreve as funcoes dummy abaixo
#
# Se o notebook NAO carregar o logger, estas funcoes dummy
# garantem que o codigo nao quebre.
# ============================================================

import threading

# Contexto de thread para evitar ZMQ errors durante execucao paralela
_in_parallel_context = threading.local()

# Funcoes dummy - serao sobrescritas se logger for carregado
def log_info(categoria, mensagem, detalhes=None):
    """Funcao dummy - sera sobrescrita se logger for carregado"""
    pass  # Nao faz nada

def log_error(categoria, mensagem, exception=None):
    """Funcao dummy - sera sobrescrita se logger for carregado"""
    print(f"ERROR [{categoria}]: {mensagem}")  # Pelo menos mostra erros

def log_warn(categoria, mensagem, detalhes=None):
    """Funcao dummy - sera sobrescrita se logger for carregado"""
    pass  # Nao faz nada

def log_success(categoria, mensagem, registros=None):
    """Funcao dummy - sera sobrescrita se logger for carregado"""
    pass  # Nao faz nada

def log_critical(categoria, mensagem, exception=None):
    """Funcao dummy - sera sobrescrita se logger for carregado"""
    print(f"CRITICAL [{categoria}]: {mensagem}")  # Mostra erros criticos

def log_api_call(endpoint, status_code, erro=None):
    """Funcao dummy - sera sobrescrita se logger for carregado"""
    pass  # Nao faz nada

def log_api_connection_error(endpoint, exception):
    """Funcao dummy - sera sobrescrita se logger for carregado"""
    pass  # Nao faz nada

def log_api_timeout(endpoint, timeout):
    """Funcao dummy - sera sobrescrita se logger for carregado"""
    pass  # Nao faz nada

def log_notebook_start(nome):
    """Funcao dummy - sera sobrescrita se logger for carregado"""
    pass  # Nao faz nada

def log_notebook_end(nome, status="SUCCESS"):
    """Funcao dummy - sera sobrescrita se logger for carregado"""
    pass  # Nao faz nada

def log_table_write(tabela, registros, modo):
    """Funcao dummy - sera sobrescrita se logger for carregado"""
    pass  # Nao faz nada

def log_quality_check(tabela, registros, issues):
    """Funcao dummy - sera sobrescrita se logger for carregado"""
    pass  # Nao faz nada

# Informa que funcoes dummy foram carregadas
print("[FUNCOES_GENERICAS] Logger nao carregado - usando funcoes dummy")

# COMMAND ----------

# DBTITLE 1,Sobre os Imports
# MAGIC %md
# MAGIC Na celula abaixo sao importadas todas as bibliotecas Python necessarias para o funcionamento
# MAGIC das funcoes genericas. Cada biblioteca tem uma funcao especifica:
# MAGIC - `requests`: faz chamadas HTTP para a API REST da Camara
# MAGIC - `time`: controla pausas entre tentativas (backoff exponencial)
# MAGIC - `json`: manipula dados no formato JSON retornado pela API
# MAGIC - `hashlib`: gera hashes MD5 para deteccao de mudancas (CDC)
# MAGIC - `datetime`: manipula datas para controle incremental

# COMMAND ----------

# DBTITLE 1,Imports e Configuracoes
# ============================================================
# IMPORTS E CONFIGURACAO
# ============================================================
# Esta celula importa todas as bibliotecas necessarias e
# define as constantes de configuracao do pipeline.
# ============================================================

# Biblioteca para fazer requisicoes HTTP a API REST
import requests

# Biblioteca para controlar tempo de espera entre retries
import time

# Biblioteca para manipular dados JSON da API
import json

# Biblioteca para gerar hashes MD5 (CDC)
import hashlib

# Biblioteca para manipulacao de datas
from datetime import datetime

# Biblioteca para paralelizacao de requisicoes HTTP
from concurrent.futures import ThreadPoolExecutor, as_completed

# Biblioteca para controle de threads
import threading

# Importa funcoes do PySpark para trabalhar com colunas
from pyspark.sql.functions import col, lit, current_timestamp

# ============================================================
# CONSTANTES DE CONFIGURACAO
# ============================================================

# URL base da API da Camara dos Deputados
API_BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

# Tamanho da pagina (quantidade de registros por requisicao)
PAGE_SIZE = 100

# Numero maximo de tentativas em caso de erro
MAX_RETRIES = 3

# Timeout das requisicoes HTTP (segundos)
REQUEST_TIMEOUT = 30

# Numero maximo de threads paralelas para fetch_api_parallel
MAX_WORKERS = 10

# Nome do catalogo no Unity Catalog
CATALOG = "uc_fast_track"

# Nome dos schemas (camadas do Medallion)
BRONZE_SCHEMA = "ft_bronze"
SILVER_SCHEMA = "ft_silver"
GOLD_SCHEMA = "ft_gold"

# ============================================================
# CACHE DE SCHEMAS (OTIMIZACAO)
# ============================================================
# Cache global para schemas inferidos por tabela
# Evita re-inferir schema a cada gravacao
_SCHEMA_CACHE = {}

# ============================================================
# RATE LIMITER GLOBAL (OTIMIZACAO)
# ============================================================
class RateLimiter:
    """Controla taxa de requisicoes para evitar 429 (rate limit)"""
    def __init__(self, max_requests_per_second=10):
        self.max_requests = max_requests_per_second
        self.requests = []
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        """Aguarda se necessario para nao ultrapassar limite"""
        with self.lock:
            now = time.time()
            # Remove requisicoes antigas (> 1 segundo)
            self.requests = [r for r in self.requests if now - r < 1.0]
            
            # Se atingiu limite, aguarda
            if len(self.requests) >= self.max_requests:
                sleep_time = 1.0 - (now - self.requests[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                self.requests = []
            
            # Registra nova requisicao
            self.requests.append(time.time())

# Instancia global do rate limiter
_rate_limiter = RateLimiter(max_requests_per_second=10)

# COMMAND ----------

# DBTITLE 1,Criacao dos Schemas
# MAGIC %md
# MAGIC ## Criacao dos Schemas no Unity Catalog
# MAGIC
# MAGIC Na celula abaixo sao criados os tres schemas do modelo Medallion (Bronze, Silver, Gold)
# MAGIC caso ainda nao existam. Cada schema representa uma camada de qualidade dos dados:
# MAGIC - **Bronze**: dados brutos da API, sem transformacao
# MAGIC - **Silver**: dados limpos, tipados e com regras de negocio
# MAGIC - **Gold**: dados agregados prontos para consumo analitico

# COMMAND ----------

# DBTITLE 1,Setup Schemas (OTIMIZADO)
# ============================================================
# CRIACAO DO CATALOGO E SCHEMAS (OTIMIZADO COM FLAG)
# ============================================================
# Usa flag em memoria para evitar reprocessamento.
# Na primeira execucao, cria os schemas.
# Nas execucoes seguintes (mesma sessao), pula essa etapa.
# ============================================================

# Verifica se ja foi executado nesta sessao
if 'SCHEMAS_CREATED' not in globals():
    print("[FUNCOES_GENERICAS] Criando schemas (primeira vez)...")
    
    # Cria catalogo se nao existir
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    
    # Cria schemas se nao existirem
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
    
    # Marca como executado
    SCHEMAS_CREATED = True
    print("[FUNCOES_GENERICAS] Schemas criados/verificados com sucesso")
else:
    print("[FUNCOES_GENERICAS] Schemas ja criados nesta sessao - pulando")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Funcoes de Ingestao da API
# MAGIC %md
# MAGIC # Funcoes de Ingestao da API
# MAGIC
# MAGIC As funcoes abaixo encapsulam toda a logica de comunicacao com a API REST
# MAGIC da Camara dos Deputados, incluindo paginacao automatica, retry com backoff
# MAGIC exponencial e tratamento detalhado de erros.

# COMMAND ----------

# DBTITLE 1,Sobre a funcao fetch_api
# MAGIC %md
# MAGIC A funcao `fetch_api()` e a funcao principal de ingestao. Ela:
# MAGIC 1. Faz requisicoes GET para a API com paginacao automatica
# MAGIC 2. Em caso de erro, tenta novamente ate 3 vezes (com espera crescente)
# MAGIC 3. Trata especificamente erros de conexao, timeout e rate-limiting
# MAGIC 4. Registra cada passo no sistema de logging para auditoria

# COMMAND ----------

# DBTITLE 1,Funcao Auxiliar Retry
# ============================================================
# FUNCAO AUXILIAR DE RETRY (OTIMIZACAO - DRY)
# ============================================================
# Funcao reutilizavel que encapsula a logica de retry com
# backoff exponencial. Elimina codigo duplicado entre
# fetch_api(), fetch_sub_endpoint() e outras funcoes.
# ============================================================

def _execute_request_with_retry(url: str, params: dict, max_retries: int = MAX_RETRIES) -> dict:
    """
    Executa requisicao HTTP com retry e backoff exponencial.
    
    Args:
        url: URL completa do endpoint
        params: Parametros de query string
        max_retries: Numero maximo de tentativas
    
    Returns:
        Dicionario JSON da resposta, ou None se falhou
    
    Raises:
        requests.exceptions.ConnectionError: Erro de conexao persistente
        requests.exceptions.Timeout: Timeout persistente
    """
    for attempt in range(max_retries):
        try:
            # Faz a requisicao GET com timeout configurado
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            
            # Se a resposta foi 200 (sucesso)
            if response.status_code == 200:
                return response.json()
            
            # Se recebeu 400 (bad request - IDs inativos/invalidos)
            elif response.status_code == 400:
                # Recurso nao existe - retorna None sem retry
                return None
            
            # Se recebeu 404 (recurso nao encontrado)
            elif response.status_code == 404:
                # Recurso nao existe - retorna None sem retry
                return None
            
            # Se recebeu 429 (rate limited)
            elif response.status_code == 429:
                # Calcula tempo de espera exponencial (2, 4, 8 segundos)
                wait_time = 2 ** attempt
                print(f"  Rate limited (429) - aguardando {wait_time}s...")
                time.sleep(wait_time)
            
            # Se recebeu erro do servidor (500, 502, 503, etc)
            elif response.status_code >= 500:
                # Aguarda com backoff exponencial antes de retry
                time.sleep(2 ** attempt)
            
            # Qualquer outro status inesperado
            else:
                # Aguarda 1 segundo antes de retry
                time.sleep(1)
        
        # Erro de conexao (rede indisponivel, DNS falhou, etc)
        except requests.exceptions.ConnectionError as e:
            # Se ainda tem tentativas restantes
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  Erro de conexao - tentando reconectar em {wait}s...")
                time.sleep(wait)
            else:
                # Todas as tentativas falharam - propaga excecao
                raise
        
        # Erro de timeout
        except requests.exceptions.Timeout as e:
            # Se ainda tem tentativas
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                # Timeout persistente - propaga excecao
                raise
        
        # Resposta da API nao e JSON valido
        except requests.exceptions.JSONDecodeError as e:
            print(f"  Resposta invalida (nao e JSON)")
            time.sleep(1)
    
    # Todas as tentativas falharam
    return None

# COMMAND ----------

# DBTITLE 1,Funcao Ingestao API
# ============================================================
# FUNCAO DE INGESTAO VIA API REST (OTIMIZADA + BUG FIX)
# ============================================================
# Esta funcao busca dados de qualquer endpoint da API da Camara
# dos Deputados. Ela percorre todas as paginas automaticamente
# e trata erros de rede, timeout e rate-limiting.
# OTIMIZACOES: Rate limiter integrado, usa funcao auxiliar de retry
# BUG FIX: Trata corretamente quando 'dados' e dict (endpoint individual)
# Retorna uma lista Python com todos os registros obtidos.
# ============================================================

def fetch_api(endpoint: str, params: dict = None, max_pages: int = 999) -> list:
    """
    Busca dados da API com paginacao automatica, retry e logging completo.
    
    Args:
        endpoint: Caminho relativo da API (ex: '/deputados')
        params: Parametros de query string
        max_pages: Numero maximo de paginas a buscar
    
    Returns:
        Lista com todos os registros de todas as paginas
    """
    # Inicializa parametros como dicionario vazio se nao fornecido
    if params is None:
        params = {}
    
    # Define quantidade de itens por pagina (padrao do projeto)
    params.setdefault('itens', PAGE_SIZE)
    
    # Lista que acumulara todos os registros de todas as paginas
    all_data = []
    
    # Contador de pagina atual (inicia na primeira)
    page = 1
    
    # Contador de erros totais para relatorio final
    total_errors = 0
    
    # Registra inicio da operacao no log
    log_info("API_FETCH_START", f"Iniciando fetch: {endpoint}", detalhes=f"params={params}")
    
    # Loop principal: percorre todas as paginas ate acabar ou atingir limite
    while page <= max_pages:
        # Define o numero da pagina atual nos parametros da requisicao
        params['pagina'] = page
        
        try:
            # OTIMIZACAO: Rate limiter global (evita 429)
            _rate_limiter.wait_if_needed()
            
            # Monta a URL completa do endpoint
            url = f"{API_BASE_URL}{endpoint}"
            
            # OTIMIZACAO: Usa funcao auxiliar de retry (DRY)
            json_data = _execute_request_with_retry(url, params)
            
            # Se retornou None (erro 400/404 ou falha total)
            if json_data is None:
                # Retorna dados parciais
                log_warn("API_FETCH_PARTIAL", f"Fetch parcial: {endpoint}", 
                        detalhes=f"Obtidos {len(all_data)} registros")
                return all_data
            
            # Extrai a lista de dados do campo 'dados'
            dados = json_data.get('dados', [])
            
            # Se nao veio dados, significa que acabaram as paginas
            if not dados:
                log_success("API_FETCH_DONE", f"Fetch completo: {endpoint}", registros=len(all_data))
                return all_data
            
            # ============================================================
            # BUG FIX: Trata corretamente quando 'dados' e dict ou list
            # ============================================================
            # API retorna:
            # - /deputados: dados = [lista de deputados]
            # - /deputados/{id}: dados = {um deputado}
            # ============================================================
            if isinstance(dados, list):
                # Caso normal: endpoint de lista (ex: /deputados)
                all_data.extend(dados)
            elif isinstance(dados, dict):
                # Caso endpoint individual (ex: /deputados/123)
                all_data.append(dados)
            else:
                # Tipo inesperado - loga e ignora
                log_warn("API_UNEXPECTED_TYPE", 
                        f"Tipo inesperado em {endpoint}: {type(dados)}", 
                        detalhes=f"Esperado list ou dict")
            
            # Verifica nos links da resposta se existe proxima pagina
            links = json_data.get('links', [])
            has_next = any(l.get('rel') == 'next' for l in links)
            
            # Se nao tem proxima pagina, retorna tudo
            if not has_next:
                log_success("API_FETCH_DONE", f"Fetch completo: {endpoint}", registros=len(all_data))
                return all_data
        
        # Erro de conexao persistente
        except requests.exceptions.ConnectionError as e:
            total_errors += 1
            log_critical("API_CONNECTION_EXHAUSTED", 
                f"Conexao esgotada para {endpoint}", exception=e)
            print(f"  CONEXAO ESGOTADA: {endpoint} - continuando com dados disponiveis.")
            return all_data
        
        # Timeout persistente
        except requests.exceptions.Timeout as e:
            total_errors += 1
            log_error("API_TIMEOUT_EXHAUSTED", f"Timeout persistente em {endpoint}", exception=e)
            print(f"  Timeout persistente em {endpoint} - continuando com dados disponiveis.")
            return all_data
        
        # Qualquer outro erro inesperado
        except Exception as e:
            total_errors += 1
            log_error("API_UNEXPECTED_ERROR", f"Erro inesperado: {type(e).__name__}", exception=e)
            print(f"  Erro inesperado em {endpoint}: {type(e).__name__}")
            return all_data
        
        # Avanca para a proxima pagina
        page += 1
    
    # Apos processar todas as paginas, registra resultado final
    if total_errors > 0:
        log_warn("API_FETCH_PARTIAL", f"Fetch parcial: {endpoint} ({total_errors} erros)", 
                detalhes=f"Obtidos {len(all_data)} registros")
    else:
        log_success("API_FETCH_DONE", f"Fetch completo: {endpoint}", registros=len(all_data))
    
    return all_data

# COMMAND ----------

# DBTITLE 1,Sobre a funcao fetch_api_parallel
# MAGIC %md
# MAGIC A funcao `fetch_api_parallel()` executa multiplas requisicoes a API em paralelo usando threads.
# MAGIC Isso aumenta drasticamente a velocidade de ingestao quando precisamos fazer milhares de requisicoes
# MAGIC (como buscar despesas de 859 deputados x 3 anos x 12 meses = ~30k requisicoes).
# MAGIC Usa ThreadPoolExecutor para processar ate 10 requisicoes simultaneamente.

# COMMAND ----------

# DBTITLE 1,Funcao Ingestao API Paralela
# ============================================================
# FUNCAO DE INGESTAO VIA API REST EM PARALELO (OTIMIZADA)
# ============================================================
# Esta funcao busca multiplos endpoints simultaneamente usando
# threads, aumentando drasticamente a velocidade de ingestao.
# Ideal para cenarios com milhares de requisicoes.
# OTIMIZAÇÃO: Usa contexto de thread para evitar ZMQ errors.
# ============================================================

def fetch_api_parallel(endpoints_list: list, max_workers: int = None) -> list:
    """
    Busca multiplos endpoints em paralelo usando threads.
    
    Args:
        endpoints_list: Lista de tuplas (endpoint, params)
        max_workers: Numero maximo de threads paralelas (default: MAX_WORKERS)
    
    Returns:
        Lista de dicionarios com resultado de cada requisicao:
        {'endpoint': str, 'params': dict, 'data': list, 'success': bool, 'error': str}
    """
    # Usa MAX_WORKERS global se nao especificado
    if max_workers is None:
        max_workers = MAX_WORKERS
    
    # Lista para acumular resultados
    results = []
    
    # Contadores para progresso
    total_requests = len(endpoints_list)
    completed = 0
    success_count = 0
    
    # Exibe inicio no THREAD PRINCIPAL (thread-safe)
    print(f"   Processando {total_requests} requisições em paralelo com {max_workers} threads...")
    
    # ATIVA CONTEXTO DE THREAD para silenciar logs individuais
    _in_parallel_context.active = True
    
    try:
        # Cria pool de threads e executa em paralelo
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Cria dicionario de futures (tarefas agendadas)
            future_to_endpoint = {
                executor.submit(fetch_api, endpoint, params): (endpoint, params)
                for endpoint, params in endpoints_list
            }
            
            # Processa conforme cada tarefa completa
            for future in as_completed(future_to_endpoint):
                endpoint, params = future_to_endpoint[future]
                completed += 1
                
                try:
                    # Obtem resultado da requisicao
                    data = future.result()
                    success_count += 1
                    
                    # Adiciona resultado com sucesso
                    results.append({
                        'endpoint': endpoint,
                        'params': params,
                        'data': data,
                        'success': True
                    })
                except Exception as e:
                    # Em caso de erro, registra e continua
                    results.append({
                        'endpoint': endpoint,
                        'params': params,
                        'data': [],
                        'success': False,
                        'error': str(e)
                    })
                
                # Exibe progresso a cada 50 requisicoes (thread-safe - main thread)
                if completed % 50 == 0 or completed == total_requests:
                    print(f"      Progresso: {completed}/{total_requests} ({success_count} sucessos)")
    
    finally:
        # DESATIVA CONTEXTO ao terminar
        _in_parallel_context.active = False
    
    # Exibe resumo final
    print(f"   Paralelo concluído: {success_count}/{total_requests} sucessos")
    
    return results

# COMMAND ----------

# DBTITLE 1,Sobre a funcao fetch_sub_endpoint
# MAGIC %md
# MAGIC A funcao `fetch_sub_endpoint()` busca sub-recursos de uma lista de IDs.
# MAGIC Por exemplo, para cada deputado busca suas despesas, ou para cada votacao busca os votos.
# MAGIC Ela controla erros consecutivos e aborta se mais de 10 seguidos falharem.

# COMMAND ----------

# DBTITLE 1,Funcao Ingestao Sub-Endpoint
# ============================================================
# FUNCAO DE INGESTAO DE SUB-ENDPOINTS
# ============================================================
# Esta funcao percorre uma lista de IDs e busca o sub-recurso
# de cada um. Exemplo: /deputados/{id}/despesas.
# Ela aborta automaticamente se ocorrerem 10 erros consecutivos
# para evitar loops infinitos em caso de API indisponivel.
# ============================================================

def fetch_sub_endpoint(ids_list: list, endpoint_template: str, id_field: str, extra_fields: dict = None) -> list:
    """
    Busca sub-endpoints para uma lista de IDs com controle de erros.
    
    Args:
        ids_list: Lista de IDs para iterar
        endpoint_template: Template do endpoint (ex: '/deputados/{id}/despesas')
        id_field: Nome do campo de ID nos dados de entrada
        extra_fields: Campos extras para adicionar a cada registro
    
    Returns:
        Lista com todos os registros de todos os sub-endpoints
    """
    # Lista para acumular todos os resultados
    all_data = []
    
    # Contador de erros consecutivos (reseta quando tem sucesso)
    consecutive_errors = 0
    
    # Limite maximo de erros consecutivos antes de abortar
    MAX_CONSECUTIVE_ERRORS = 10
    
    # Registra inicio da operacao no log
    log_info("SUB_FETCH_START", f"Buscando sub-endpoint para {len(ids_list)} IDs", 
             detalhes=endpoint_template)
    
    # Loop: percorre cada ID da lista
    for i, item_id in enumerate(ids_list):
        try:
            # Monta o endpoint substituindo {id} pelo ID atual
            endpoint = endpoint_template.format(id=item_id)
            
            # Chama a funcao principal de fetch para este endpoint
            dados = fetch_api(endpoint)
            
            # Se retornou dados, adiciona campos auxiliares
            if dados:
                # Para cada registro retornado
                for d in dados:
                    # Adiciona o ID do item pai como campo de referencia
                    d[f'_{id_field}'] = item_id
                    # Se tem campos extras, adiciona cada um
                    if extra_fields:
                        for k, v in extra_fields.items():
                            d[k] = v
                
                # Adiciona os dados ao acumulador
                all_data.extend(dados)
                
                # Reseta contador de erros (houve sucesso)
                consecutive_errors = 0
            
            # Exibe progresso a cada 50 itens
            if (i + 1) % 50 == 0:
                print(f"    Progresso: {i+1}/{len(ids_list)} ({len(all_data)} registros)")
                
        # Erro de conexao - aborta imediatamente
        except requests.exceptions.ConnectionError as e:
            # Registra erro critico
            log_api_connection_error(endpoint_template, e)
            # Informa usuario e para o loop
            print(f"  ERRO DE CONEXAO no item {item_id} - abortando lote")
            break
            
        # Qualquer outro erro
        except Exception as e:
            # Incrementa contador de erros consecutivos
            consecutive_errors += 1
            # Registra o erro no log
            log_error("SUB_FETCH_ERROR", f"Erro no item {item_id}: {str(e)[:60]}", exception=e)
            # Informa o usuario
            print(f"  Erro no item {item_id}: {str(e)[:60]}")
            
            # Se atingiu limite de erros consecutivos, aborta
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                # Registra aborto no log
                log_critical("SUB_FETCH_ABORT", 
                    f"ABORTANDO: {MAX_CONSECUTIVE_ERRORS} erros consecutivos em {endpoint_template}")
                # Informa o usuario
                print(f"  ABORTANDO: {MAX_CONSECUTIVE_ERRORS} erros consecutivos - API possivelmente indisponivel")
                break
    
    # Registra resultado final no log
    log_success("SUB_FETCH_DONE", f"Sub-fetch concluido: {len(all_data)} registros de {len(ids_list)} IDs")
    
    # Retorna todos os dados acumulados
    return all_data

# COMMAND ----------

# DBTITLE 1,Funcoes de Gravacao em Tabelas
# MAGIC %md
# MAGIC # Funcoes de Gravacao em Tabelas Delta
# MAGIC
# MAGIC As funcoes abaixo gravam DataFrames nas tabelas Delta Lake do Unity Catalog.
# MAGIC Cada funcao e especializada em uma camada do modelo Medallion (Bronze, Silver, Gold).

# COMMAND ----------

# DBTITLE 1,Sobre a funcao save_to_bronze
# MAGIC %md
# MAGIC A funcao `save_to_bronze()` grava dados brutos na camada Bronze.
# MAGIC Ela adiciona campos de auditoria (`_ingested_at`, `_source_endpoint`, `_batch_id`)
# MAGIC e permite tanto modo `overwrite` (substitui tudo) quanto `append` (adiciona).

# COMMAND ----------

# DBTITLE 1,Funcao Gravar Bronze
# ============================================================
# FUNCAO PARA GRAVAR DADOS NA CAMADA BRONZE (OTIMIZADA)
# ============================================================
# Recebe uma lista de dicionarios (dados da API), converte em
# DataFrame Spark, adiciona campos de auditoria e grava na
# tabela Delta correspondente no schema Bronze.
# OTIMIZACOES: usa len(data) em vez de df.count(), cache de schemas
# ============================================================

def save_to_bronze(data: list, table_name: str, endpoint: str = "", mode: str = "overwrite") -> int:
    """
    Grava dados brutos na camada Bronze com campos de auditoria.
    
    Args:
        data: Lista de dicionarios com dados da API
        table_name: Nome da tabela destino (sem catalog/schema)
        endpoint: Endpoint de origem (para auditoria)
        mode: Modo de gravacao ('overwrite' ou 'append')
    
    Returns:
        Numero de registros gravados
    """
    # OTIMIZACAO: Conta registros ANTES de criar DataFrame
    row_count = len(data)
    
    # Se a lista de dados esta vazia, nao grava nada
    if row_count == 0:
        # Registra aviso no log
        log_warn("SAVE_BRONZE", f"Nenhum dado para gravar em {table_name}")
        # Informa o usuario
        print(f"  Nenhum dado para gravar em {table_name}")
        # Retorna zero registros
        return 0
    
    from pyspark.sql.types import StructType, StructField, StringType
    
    # OTIMIZACAO: Usa cache de schemas se disponivel
    if table_name in _SCHEMA_CACHE:
        schema = _SCHEMA_CACHE[table_name]
    else:
        # Primeira vez: infere schema APENAS do primeiro registro
        # (nao precisa iterar todos os registros)
        sample_keys = data[0].keys()
        schema_fields = [StructField(key, StringType(), True) for key in sorted(sample_keys)]
        schema = StructType(schema_fields)
        # Armazena no cache para proximas execucoes
        _SCHEMA_CACHE[table_name] = schema
    
    # Converte a lista de dicionarios em DataFrame Spark com schema explícito
    df = spark.createDataFrame(data, schema=schema)
    
    # Adiciona coluna com timestamp da ingestao (auditoria)
    df = df.withColumn("_ingested_at", current_timestamp())
    
    # Adiciona coluna com o endpoint de origem (rastreabilidade)
    df = df.withColumn("_source_endpoint", lit(endpoint))
    
    # Adiciona coluna com ID do batch (agrupamento por execucao)
    df = df.withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    
    # Monta o nome completo da tabela (catalog.schema.table)
    full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    
    # Grava o DataFrame na tabela Delta com modo especificado
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(full_table)
    
    # OTIMIZACAO: NAO faz df.count() - ja sabemos o valor de len(data)
    # Economiza um full scan da tabela
    
    # Registra a gravacao no log
    log_table_write(full_table, row_count, mode)
    
    # Informa o usuario do resultado
    print(f"  {full_table}: {row_count} registros ({mode})")
    
    # Retorna a quantidade de registros gravados
    return row_count

# COMMAND ----------

# DBTITLE 1,Sobre a funcao merge_to_silver
# MAGIC %md
# MAGIC A funcao `merge_to_silver()` faz um MERGE (upsert) na camada Silver.
# MAGIC Ela compara os dados novos com os existentes usando uma chave primaria,
# MAGIC e atualiza registros existentes ou insere novos conforme necessario.

# COMMAND ----------

# DBTITLE 1,Funcao Merge Silver
# ============================================================
# FUNCAO DE MERGE INCREMENTAL NA CAMADA SILVER
# ============================================================
# Executa um MERGE (upsert) na tabela Silver usando a chave
# primaria especificada. Registros existentes sao atualizados,
# novos registros sao inseridos. Garante idempotencia.
# ============================================================

def merge_to_silver(df, table_name: str, key_columns: list, schema: str = None):
    """
    Faz MERGE (upsert) na camada Silver.
    
    Args:
        df: DataFrame com dados novos
        table_name: Nome da tabela destino
        key_columns: Lista de colunas que formam a chave primaria
        schema: Schema destino (padrao: SILVER_SCHEMA)
    """
    # Define o schema (usa Silver como padrao)
    target_schema = schema or SILVER_SCHEMA
    
    # Monta nome completo da tabela
    full_table = f"{CATALOG}.{target_schema}.{table_name}"
    
    # Registra temporariamente o DataFrame como view para usar em SQL
    df.createOrReplaceTempView("__incoming_data")
    
    # Verifica se a tabela destino ja existe no catalogo
    table_exists = spark.catalog.tableExists(full_table)
    
    # Se a tabela nao existe ainda, cria com todos os dados
    if not table_exists:
        # Grava como nova tabela Delta
        df.write.format("delta").mode("overwrite").saveAsTable(full_table)
        # Conta registros gravados
        row_count = df.count()
        # Registra no log
        log_table_write(full_table, row_count, "create")
        # Informa usuario
        print(f"  {full_table}: CRIADA com {row_count} registros")
        return
    
    # Monta a condicao de JOIN baseada nas colunas-chave
    join_condition = " AND ".join([f"target.{k} = source.{k}" for k in key_columns])
    
    # Obtem lista de todas as colunas para o UPDATE
    all_columns = df.columns
    
    # Monta o SET do UPDATE (atualiza todas as colunas)
    update_set = ", ".join([f"target.{c} = source.{c}" for c in all_columns])
    
    # Monta o INSERT com todas as colunas
    insert_cols = ", ".join(all_columns)
    insert_vals = ", ".join([f"source.{c}" for c in all_columns])
    
    # Monta e executa o comando MERGE SQL
    merge_sql = f"""
        MERGE INTO {full_table} AS target
        USING __incoming_data AS source
        ON {join_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    
    # Executa o MERGE no Spark
    spark.sql(merge_sql)
    
    # Conta registros apos o merge
    row_count = spark.table(full_table).count()
    
    # Registra no log
    log_table_write(full_table, row_count, "merge")
    
    # Informa usuario
    print(f"  {full_table}: MERGE concluido ({row_count} registros total)")

# COMMAND ----------

# DBTITLE 1,Sobre a funcao save_to_gold
# MAGIC %md
# MAGIC A funcao `save_to_gold()` grava dados analiticos na camada Gold.
# MAGIC Ela sempre faz overwrite (substitui toda a tabela) pois os dados Gold
# MAGIC sao recalculados a cada execucao com base na Silver completa.

# COMMAND ----------

# DBTITLE 1,Funcao Gravar Gold
# ============================================================
# FUNCAO PARA GRAVAR DADOS NA CAMADA GOLD (OTIMIZADA)
# ============================================================
# Grava um DataFrame na tabela Gold especificada.
# Sempre usa modo overwrite pois Gold e recalculada.
# ============================================================

def save_to_gold(df, table_name: str):
    """
    Grava DataFrame na camada Gold (analitica).
    
    Args:
        df: DataFrame com dados agregados/analiticos
        table_name: Nome da tabela destino
    """
    # Monta nome completo da tabela no Unity Catalog
    full_table = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
    
    # OTIMIZACAO: Cacheia DataFrame antes de gravar
    # Isso evita recomputacao durante count() posterior
    df = df.cache()
    
    # Grava o DataFrame substituindo dados anteriores
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table)
    
    # Conta registros gravados (necessario - nao temos len() para DataFrames)
    # Como o DF esta cacheado, count() e rapido
    row_count = df.count()
    
    # Libera cache apos uso
    df.unpersist()
    
    # Registra no log
    log_table_write(full_table, row_count, "overwrite")
    
    # Informa usuario
    print(f"  {full_table}: {row_count} registros (overwrite)")

# COMMAND ----------

# DBTITLE 1,Controle de Carga Incremental
# MAGIC %md
# MAGIC # Controle de Carga Incremental (Watermark)
# MAGIC
# MAGIC As funcoes abaixo gerenciam o controle incremental do pipeline.
# MAGIC Um "watermark" e um marcador que indica ate onde os dados ja foram processados
# MAGIC (por exemplo, a ultima data ou o ultimo ID ingerido). Na proxima execucao,
# MAGIC o pipeline busca apenas dados posteriores ao watermark.

# COMMAND ----------

# DBTITLE 1,Sobre as funcoes de watermark
# MAGIC %md
# MAGIC - `get_watermark()`: Busca o ultimo valor processado para um endpoint
# MAGIC - `set_watermark()`: Salva o novo valor apos processamento bem-sucedido
# MAGIC
# MAGIC Os watermarks sao armazenados na tabela `_pipeline_control` no schema Bronze.

# COMMAND ----------

# DBTITLE 1,Funcao Controle Incremental
# ============================================================
# CONTROLE DE CARGA INCREMENTAL (WATERMARK)
# ============================================================
# Estas funcoes leem e gravam o ultimo valor processado
# na tabela de controle. Isso permite que o pipeline
# processe apenas dados novos a cada execucao.
# ============================================================

def get_watermark(endpoint: str, default_value: str = None) -> dict:
    """
    Obtem o ultimo valor processado (watermark) para um endpoint.
    
    Args:
        endpoint: Identificador do endpoint/processo
        default_value: Valor padrao se nao existir watermark
    
    Returns:
        Dicionario com chaves 'last_value', 'last_date', 'last_id'
        Todas apontam para o mesmo valor para compatibilidade.
    """
    # Monta nome completo da tabela de controle
    control_table = f"{CATALOG}.{BRONZE_SCHEMA}._pipeline_control"
    
    # Resultado padrao (vazio)
    empty_result = {"last_value": None, "last_date": None, "last_id": None}
    
    try:
        # Tenta ler o watermark da tabela de controle
        result = spark.sql(f"""
            SELECT last_value 
            FROM {control_table} 
            WHERE endpoint = '{endpoint}'
        """).collect()
        
        # Se encontrou registro, retorna o valor em formato dict
        if result and result[0][0]:
            # Extrai o valor da primeira linha
            value = result[0][0]
            # Retorna dict com o valor em todas as chaves
            return {"last_value": value, "last_date": value, "last_id": value}
        else:
            # Nao encontrou registro - e primeira execucao deste endpoint
            log_info("WATERMARK", f"Sem watermark para {endpoint} - primeira execucao")
            # Retorna dict vazio
            return empty_result
            
    except Exception as e:
        # Tabela pode nao existir ainda (primeira execucao geral)
        log_info("WATERMARK", f"Tabela de controle nao existe - primeira execucao")
        # Informa usuario
        print(f"  Tabela de controle nao existe - primeira execucao")
        # Retorna dict vazio
        return empty_result


def set_watermark(endpoint: str, value: str = None, last_date: str = None, last_id = None):
    """
    Salva o novo watermark apos processamento bem-sucedido.
    
    Args:
        endpoint: Identificador do endpoint/processo
        value: Novo valor do watermark
    """
    # Monta nome completo da tabela de controle
    control_table = f"{CATALOG}.{BRONZE_SCHEMA}._pipeline_control"
    
    # Resolve o valor a partir dos parametros fornecidos
    final_value = value or last_date or str(last_id) if last_id else value
    
    # Cria DataFrame com o novo valor
    data = [{"endpoint": endpoint, "last_value": final_value, "updated_at": datetime.now().isoformat()}]
    df = spark.createDataFrame(data)
    
    # Cria a tabela se nao existir
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {control_table} (
            endpoint STRING,
            last_value STRING,
            updated_at STRING
        ) USING DELTA
    """)
    
    # Registra como view temporaria
    df.createOrReplaceTempView("__new_watermark")
    
    # Faz MERGE: atualiza se existe, insere se nao existe
    spark.sql(f"""
        MERGE INTO {control_table} AS target
        USING __new_watermark AS source
        ON target.endpoint = source.endpoint
        WHEN MATCHED THEN UPDATE SET 
            target.last_value = source.last_value,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    # Registra no log
    log_info("WATERMARK_SET", f"Watermark atualizado: {endpoint} = {value}")

# COMMAND ----------

# DBTITLE 1,Validacao de Qualidade
# MAGIC %md
# MAGIC # Validacao de Qualidade dos Dados
# MAGIC
# MAGIC A funcao abaixo verifica a qualidade dos dados apos ingestao ou transformacao,
# MAGIC checando registros nulos, duplicados e volume minimo.

# COMMAND ----------

# DBTITLE 1,Sobre a funcao check_quality
# MAGIC %md
# MAGIC A funcao `check_quality()` realiza tres verificacoes automaticas:
# MAGIC 1. **Contagem total** de registros (deve ser > 0)
# MAGIC 2. **Nulos em colunas criticas** (campos obrigatorios)
# MAGIC 3. **Duplicatas na chave primaria** (devem ser zero)
# MAGIC
# MAGIC O resultado e registrado no log para auditoria.

# COMMAND ----------

# DBTITLE 1,Funcao Qualidade Dados
# ============================================================
# VALIDACAO E QUALIDADE DOS DADOS (OTIMIZADO - SINGLE PASS)
# ============================================================
# Verifica qualidade basica: nulos em colunas criticas,
# duplicatas na chave primaria e volume minimo.
# OTIMIZACAO: Usa agregacoes PySpark em UMA UNICA PASSADA
# ao inves de multiplos df.count() (70-90% mais rapido)
# ============================================================

def check_quality(df, table_name: str, key_columns: list = None, critical_columns: list = None) -> dict:
    """
    Valida qualidade dos dados com agregacoes em single-pass.
    
    Args:
        df: DataFrame a validar
        table_name: Nome da tabela (para log)
        key_columns: Colunas da chave primaria (verifica duplicatas)
        critical_columns: Colunas obrigatorias (verifica nulos)
    
    Returns:
        Dicionario com metricas de qualidade
    """
    from pyspark.sql.functions import count, when, countDistinct, concat_ws
    
    # Dicionario para armazenar metricas
    metrics = {"table": table_name, "issues": []}
    
    # OTIMIZACAO: Prepara TODAS as agregacoes de uma vez
    aggs = [count("*").alias("total")]
    
    # Adiciona contadores de nulos para cada coluna critica
    if critical_columns:
        for col_name in critical_columns:
            aggs.append(
                count(when(col(col_name).isNull(), 1)).alias(f"nulls_{col_name}")
            )
    
    # Adiciona contador de distintos para chave primaria
    if key_columns:
        if len(key_columns) == 1:
            aggs.append(countDistinct(key_columns[0]).alias("distinct_keys"))
        else:
            # Para multiplas colunas, cria chave composta
            aggs.append(
                countDistinct(concat_ws("|||", *key_columns)).alias("distinct_keys")
            )
    
    # EXECUTA TUDO EM UMA UNICA PASSADA (single scan)
    stats = df.agg(*aggs).collect()[0]
    
    # Extrai metricas do resultado
    total = stats["total"]
    metrics["total_records"] = total
    
    # Verifica se tem dados
    if total == 0:
        metrics["issues"].append(f"ALERTA: {table_name} esta VAZIA (0 registros)")
    
    # Verifica nulos em colunas criticas
    if critical_columns:
        for col_name in critical_columns:
            null_count = stats[f"nulls_{col_name}"]
            if null_count > 0:
                pct = (null_count / total * 100) if total > 0 else 0
                metrics["issues"].append(f"  Nulos em {col_name}: {null_count} ({pct:.1f}%)")
    
    # Verifica duplicatas na chave primaria
    if key_columns and total > 0:
        distinct_count = stats["distinct_keys"]
        duplicates = total - distinct_count
        if duplicates > 0:
            metrics["issues"].append(f"  Duplicatas na chave {key_columns}: {duplicates}")
    
    # Exibe resultado no console
    status = "OK" if not metrics["issues"] else "ALERTA"
    print(f"  Qualidade [{table_name}]: {total} registros - {status}")
    
    # Exibe cada problema encontrado
    for issue in metrics["issues"]:
        print(issue)
    
    # Registra no log
    log_quality_check(table_name, total, metrics["issues"])
    
    # Retorna metricas para uso posterior
    return metrics

# COMMAND ----------

# DBTITLE 1,Variaveis de Status do Pipeline
# MAGIC %md
# MAGIC # Variaveis de Status
# MAGIC
# MAGIC As variaveis abaixo sao usadas para controlar o tempo de execucao e
# MAGIC acumular o status de cada tabela processada durante a execucao do notebook.

# COMMAND ----------

# DBTITLE 1,Variaveis Status
# ============================================================
# VARIAVEIS DE STATUS DO PIPELINE
# ============================================================
# Estas variaveis controlam o tempo de execucao e acumulam
# informacoes sobre cada tabela processada. Sao usadas pela
# funcao finalizar_notebook() para gerar o resumo final.
# ============================================================

# ============================================================
# FUNCAO STATUS_PROCESSAMENTO
# ============================================================
# Registra o status de uma tabela processada na lista de
# acompanhamento para o resumo final do notebook.
# ============================================================

def status_processamento(tabela: str, registros: int):
    """
    Registra status de processamento de uma tabela.
    
    Args:
        tabela: Nome da tabela processada
        registros: Quantidade de registros processados
    """
    # Adiciona a tabela e contagem a lista de status
    status_list.append({"tabela": tabela, "registros": registros})


# Registra o horario de inicio da execucao do notebook
TInicio = datetime.now()

# Lista para acumular status de cada tabela processada
status_list = []

# COMMAND ----------

# DBTITLE 1,Funcao Finalizar Notebook
# MAGIC %md
# MAGIC ## Funcao de Finalizacao
# MAGIC
# MAGIC A funcao `finalizar_notebook()` e chamada no final de cada notebook para
# MAGIC exibir um resumo com tempo total de execucao e tabelas processadas.

# COMMAND ----------

# DBTITLE 1,Funcao Finalizar
# ============================================================
# FUNCAO DE FINALIZACAO DO NOTEBOOK
# ============================================================
# Calcula o tempo total de execucao e exibe um resumo com
# todas as tabelas processadas e seus volumes.
# ============================================================

def finalizar_notebook():
    """Calcula tempo total e exibe resumo com logging"""
    # Registra horario de fim
    TFim = datetime.now()
    
    # Calcula duracao total em minutos
    duracao = (TFim - TInicio).total_seconds() / 60
    
    # Exibe separador visual
    print(f"\n{'='*60}")
    
    # Exibe tempo total formatado
    print(f"Tempo total: {duracao:.2f} minutos")
    
    # Exibe quantidade de tabelas processadas
    print(f"Tabelas processadas: {len(status_list)}")
    
    # Lista cada tabela com seu volume
    for s in status_list:
        print(f"   - {s['tabela']}: {s['registros']} registros")
    
    # Fecha separador visual
    print(f"{'='*60}")
    
    # Registra fim do notebook no log
    log_notebook_end(_CURRENT_NOTEBOOK, status="SUCCESS")

# COMMAND ----------

# DBTITLE 1,Função paralelo
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Adicione ao início do FUNCOES_GENERICAS
MAX_WORKERS = 10  # Número de threads paralelas

# Flag global para controlar se está em contexto paralelo
_in_parallel_context = threading.local()

def fetch_api_parallel(endpoints_list, max_workers=MAX_WORKERS):
    """
    Busca múltiplos endpoints em paralelo usando threads.
    Desabilita persistência de logs no Delta durante execução paralela.
    
    Args:
        endpoints_list: Lista de tuplas (endpoint, params)
        max_workers: Número máximo de threads paralelas
    
    Returns:
        Lista de resultados na mesma ordem dos endpoints
    """
    results = []
    
    # Buffer para logs durante execução paralela
    log_buffer = []
    
    def fetch_with_context(endpoint, params):
        """Wrapper que marca contexto paralelo"""
        # Marca que esta thread está em contexto paralelo
        _in_parallel_context.active = True
        try:
            data = fetch_api(endpoint, params)
            return {
                'endpoint': endpoint,
                'params': params,
                'data': data,
                'success': True
            }
        except Exception as e:
            # Apenas loga no console, não tenta gravar no Delta
            print(f"WARN: Erro em {endpoint}: {str(e)[:100]}")
            return {
                'endpoint': endpoint,
                'params': params,
                'data': [],
                'success': False,
                'error': str(e)
            }
        finally:
            # Limpa o contexto
            _in_parallel_context.active = False
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Cria dicionário de futures
        future_to_endpoint = {
            executor.submit(fetch_with_context, endpoint, params): (endpoint, params)
            for endpoint, params in endpoints_list
        }
        
        # Processa conforme completam
        for future in as_completed(future_to_endpoint):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                endpoint, params = future_to_endpoint[future]
                print(f"WARN: Falha crítica em {endpoint}: {str(e)[:100]}")
                results.append({
                    'endpoint': endpoint,
                    'params': params,
                    'data': [],
                    'success': False,
                    'error': str(e)
                })
    
    return results

# COMMAND ----------

# DBTITLE 1,Função aborta
def fetch_api_with_circuit_breaker(endpoint, params=None):
    """
    Wrapper que verifica saúde da API antes de tentar
    """
    global consecutive_failures
    
    # Se muitas falhas consecutivas, testa conexão primeiro
    if consecutive_failures > 5:
        try:
            # Testa endpoint simples
            response = requests.get(f"{API_BASE_URL}/deputados", 
                                   params={"itens": 1}, 
                                   timeout=5)
            if response.status_code == 200:
                consecutive_failures = 0  # API voltou
            else:
                raise Exception("API indisponível")
        except:
            raise Exception("Circuit breaker aberto - API fora do ar")
    
    return fetch_api(endpoint, params)
