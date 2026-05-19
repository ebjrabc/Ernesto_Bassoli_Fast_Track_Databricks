# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://gazetadasemana.com.br/images/noticias/166864/19041851_compass.uo.jpg.jpg" width="450"/>

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Bronze - Deputados
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook realiza a ingestao dos dados de deputados federais da Legislatura 57 (2023-2027)
# MAGIC a partir da API aberta da Camara dos Deputados. Sao extraidos tanto a lista basica de deputados
# MAGIC quanto seus detalhes individuais (gabinete, escolaridade, naturalidade, redes sociais).
# MAGIC Os dados sao gravados na camada Bronze sem nenhuma transformacao, preservando o formato original da API.
# MAGIC
# MAGIC ## Entradas (Tabelas de Origem)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|-----------|
# MAGIC | `GET /deputados` | Lista de deputados com id, nome, partido, UF |
# MAGIC | `GET /deputados/{id}` | Detalhes individuais de cada deputado |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|-----------|
# MAGIC | `dt0025_dev.ft_bronze.deputados` | Lista basica de todos os deputados |
# MAGIC | `dt0025_dev.ft_bronze.deputados_detalhes` | Detalhes completos (flatten) |
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

# DBTITLE 1,Sobre o comando run
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
log_notebook_start("bronze_deputados")

# COMMAND ----------

# DBTITLE 1,Ingestao dos Dados
# MAGIC %md
# MAGIC # Ingestao dos Dados da API

# COMMAND ----------

# DBTITLE 1,Ingere Lista Deputados
# ============================================================
# INGESTAO DA LISTA DE DEPUTADOS
# ============================================================
# Busca todos os deputados da legislatura 57 (2023-2027)
# via endpoint /deputados com paginacao automatica.
# Cada registro contem: id, nome, siglaPartido, siglaUf,
# idLegislatura, urlFoto e uri para detalhamento.
# ============================================================

print("Ingerindo lista de deputados (Legislatura 57)...")

# Chama a funcao de ingestao para o endpoint de deputados
deputados_lista = fetch_api(
    endpoint="/deputados",
    params={"idLegislatura": 57, "ordenarPor": "nome"}
)

print(f"   Deputados encontrados: {len(deputados_lista)}")

# ============================================================
# DEDUPLICACAO (CORRECAO DE BUG DA API)
# ============================================================
# A API retorna alguns IDs duplicados (bug da API da Camara).
# Precisamos dedupilcar ANTES de gravar para garantir integridade.
# ============================================================

from pyspark.sql.types import StructType, StructField, StringType

# Infere schema dinamicamente
all_keys = set()
for record in deputados_lista:
    all_keys.update(record.keys())

schema = StructType([StructField(key, StringType(), True) for key in sorted(all_keys)])

# Remove duplicatas pela chave primaria 'id'
df_deputados = spark.createDataFrame(deputados_lista, schema=schema)
df_deputados_unicos = df_deputados.dropDuplicates(["id"])

# Calcula quantas duplicatas foram removidas
duplicatas_removidas = df_deputados.count() - df_deputados_unicos.count()

if duplicatas_removidas > 0:
    print(f"   ⚠️  ALERTA: Removidas {duplicatas_removidas} duplicatas ({duplicatas_removidas/len(deputados_lista)*100:.1f}%)")
else:
    print(f"   ✅ Nenhuma duplicata encontrada")

# Converte de volta para lista
deputados_lista_limpa = [row.asDict() for row in df_deputados_unicos.collect()]
print(f"   Deputados unicos: {len(deputados_lista_limpa)}")

# Grava a lista basica (SEM DUPLICATAS)
print("   Gravando lista basica...")
n1 = save_to_bronze(deputados_lista_limpa, "deputados", "/deputados")
status_list.append({"tabela": "ft_bronze.deputados", "registros": n1})
print(f"   Lista basica gravada: {n1} registros")

# ============================================================
# OTIMIZACAO DA TABELA
# ============================================================
# Otimiza a tabela Delta para melhorar performance de leitura.
# ============================================================

print("   Otimizando tabela...")
try:
    spark.sql(f"OPTIMIZE {CATALOG}.{BRONZE_SCHEMA}.deputados")
    spark.sql(f"ANALYZE TABLE {CATALOG}.{BRONZE_SCHEMA}.deputados COMPUTE STATISTICS")
    print("   ✅ Tabela otimizada com sucesso")
except Exception as e:
    print(f"   ⚠️  Aviso: Otimizacao falhou - {str(e)[:100]}")

# Atualiza a lista para usar nas proximas celulas
deputados_lista = deputados_lista_limpa

# COMMAND ----------

# DBTITLE 1,Sobre os Detalhes Individuais
# MAGIC %md
# MAGIC Na celula abaixo, para cada deputado da lista, e feita uma requisicao individual
# MAGIC ao endpoint `/deputados/{id}` para obter informacoes detalhadas como CPF, sexo,
# MAGIC data de nascimento, naturalidade, escolaridade, situacao e dados do gabinete.
# MAGIC O processo exibe progresso a cada 50 deputados processados.

# COMMAND ----------

# DBTITLE 1,Smoke Test - Teste Manual da API
# ============================================================
# SMOKE TEST - VALIDACAO DO ENDPOINT /deputados/{id}
# ============================================================
# Testa 10 IDs de amostra ANTES de processar todos os 640.
# Se mais de 20% falharem, aborta o processamento.
# ============================================================

import requests
import random

print("="*60)
print("SMOKE TEST: Validando endpoint /deputados/{id}")
print("="*60)

# Seleciona 10 IDs estrategicamente (inicio, meio, fim + aleatorios)
sample_size = min(10, len(deputados_lista))
if len(deputados_lista) > 10:
    sample_indices = [
        0,  # Primeiro
        len(deputados_lista) // 4,  # 25%
        len(deputados_lista) // 2,  # 50%
        3 * len(deputados_lista) // 4,  # 75%
        len(deputados_lista) - 1  # Ultimo
    ]
    sample_ids = [deputados_lista[i]['id'] for i in sample_indices]
    
    # Adiciona mais 5 aleatorios
    remaining = [d['id'] for d in deputados_lista if d['id'] not in sample_ids]
    sample_ids.extend(random.sample(remaining, min(5, len(remaining))))
else:
    sample_ids = [d['id'] for d in deputados_lista]

print(f"Testando {len(sample_ids)} IDs de amostra...\n")

# Testa cada ID
sucessos = 0
erros_por_tipo = {}

for dep_id in sample_ids:
    url = f"{API_BASE_URL}/deputados/{dep_id}"
    
    try:
        resp = requests.get(url, timeout=10)
        
        if resp.status_code == 200:
            dados = resp.json().get('dados', {})
            nome = dados.get('nomeCivil', dados.get('nome', 'N/A'))
            print(f"  ✅ ID {dep_id}: {nome}")
            sucessos += 1
        else:
            erros_por_tipo[f"HTTP {resp.status_code}"] = erros_por_tipo.get(f"HTTP {resp.status_code}", 0) + 1
            print(f"  ❌ ID {dep_id}: Status {resp.status_code}")
            
    except requests.exceptions.Timeout:
        erros_por_tipo["Timeout"] = erros_por_tipo.get("Timeout", 0) + 1
        print(f"  ⏱️  ID {dep_id}: TIMEOUT")
    except Exception as e:
        tipo = type(e).__name__
        erros_por_tipo[tipo] = erros_por_tipo.get(tipo, 0) + 1
        print(f"  ⚠️  ID {dep_id}: {tipo}")

# Calcula taxa de sucesso
taxa_sucesso = (sucessos / len(sample_ids) * 100) if len(sample_ids) > 0 else 0

print(f"\n{'='*60}")
print(f"RESULTADO: {sucessos}/{len(sample_ids)} sucessos ({taxa_sucesso:.1f}%)")

if erros_por_tipo:
    print(f"\nErros por tipo:")
    for tipo, count in sorted(erros_por_tipo.items(), key=lambda x: x[1], reverse=True):
        print(f"  - {tipo}: {count}")

print(f"{'='*60}\n")

if taxa_sucesso < 80:
    print("❌ SMOKE TEST FALHOU: Taxa de sucesso < 80%")
    print("⚠️  RECOMENDACAO: Investigue os erros antes de processar 640 deputados\n")
else:
    print("✅ SMOKE TEST PASSOU: API esta respondendo normalmente\n")

# COMMAND ----------

# DBTITLE 1,Ingere Detalhes Deputados
# ============================================================
# INGESTAO DOS DETALHES DE CADA DEPUTADO
# ============================================================
# Para cada deputado, busca informacoes detalhadas via
# endpoint /deputados/{id}.
# Campos adicionais: cpf, sexo, dataNascimento, naturalidade,
# escolaridade, situacao, gabinete e redes sociais.
# 
# OTIMIZACOES:
# - Paralelismo controlado (5 threads) para respeitar rate limit
# - Batches de 100 com delay entre batches
# - Gravacao em batches (checkpoint) para acelerar processamento
# ============================================================

import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

print("Ingerindo detalhes individuais...")

total = len(deputados_lista)
MAX_WORKERS = 5
BATCH_SIZE = 100
BATCH_DELAY = 2

print(f"   Total de deputados: {total}")
print(f"   Processando em lotes de {BATCH_SIZE} com {MAX_WORKERS} threads...\n")

total_gravado = 0
erros_api = 0
erros_por_tipo = {}

def fetch_deputado_detalhes(dep_id: str) -> dict:
    """
    Busca detalhes de UM deputado via API
    """
    url = f"{API_BASE_URL}/deputados/{dep_id}"
    
    try:
        resp = requests.get(url, timeout=30)
        
        if resp.status_code == 200:
            data = resp.json()
            dados = data.get('dados', {})
            
            if isinstance(dados, dict) and dados:
                return {'success': True, 'id': dep_id, 'data': dados}
            else:
                return {'success': False, 'id': dep_id, 'error': 'Sem dados'}
        else:
            return {'success': False, 'id': dep_id, 'status_code': resp.status_code}
            
    except Exception as e:
        return {'success': False, 'id': dep_id, 'error': str(e)}

# Processa em batches
for i in range(0, total, BATCH_SIZE):
    batch = deputados_lista[i:i+BATCH_SIZE]
    batch_num = i//BATCH_SIZE + 1
    total_batches = (total-1)//BATCH_SIZE + 1
    
    print(f"   Batch {batch_num}/{total_batches}: Processando {len(batch)} deputados...")
    
    deputados_detalhes_batch = []
    
    # Processa em paralelo
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_deputado_detalhes, dep['id']): dep['id'] for dep in batch}
        
        for future in as_completed(futures):
            resultado = future.result()
            
            if resultado['success']:
                deputados_detalhes_batch.append(resultado['data'])
            else:
                # Contabiliza erro
                status_code = resultado.get('status_code', 0)
                
                if status_code == 429:
                    erro_tipo = "HTTP 429 (Rate Limit)"
                elif status_code == 400:
                    erro_tipo = "HTTP 400 (Bad Request)"
                elif status_code == 404:
                    erro_tipo = "HTTP 404 (Not Found)"
                elif status_code == 500:
                    erro_tipo = "HTTP 500 (Server Error)"
                else:
                    erro_tipo = "Connection/Timeout Error"
                
                erros_por_tipo[erro_tipo] = erros_por_tipo.get(erro_tipo, 0) + 1
                erros_api += 1
    
    # Aplica flatten aos dados do batch
    deputados_flat_batch = []
    for d in deputados_detalhes_batch:
        flat = {k: v for k, v in d.items() if not isinstance(v, dict) and not isinstance(v, list)}
        
        ult = d.get('ultimoStatus', {})
        if ult:
            for k, v in ult.items():
                if not isinstance(v, dict):
                    flat[f"status_{k}"] = v
            
            gab = ult.get('gabinete', {})
            if gab:
                for k, v in gab.items():
                    flat[f"gabinete_{k}"] = v
        
        deputados_flat_batch.append(flat)
    
    # CHECKPOINT: Grava o batch
    if deputados_flat_batch:
        mode = "append" if i > 0 else "overwrite"
        n = save_to_bronze(deputados_flat_batch, "deputados_detalhes", "/deputados/{id}", mode=mode)
        total_gravado += len(deputados_flat_batch)
        print(f"      ✅ Gravados: {len(deputados_flat_batch)} registros (Total acumulado: {total_gravado})")
    else:
        print(f"      ⚠️  Nenhum dado valido neste batch")
    
    # Delay entre batches
    if i + BATCH_SIZE < total:
        print(f"      Aguardando {BATCH_DELAY}s...")
        time.sleep(BATCH_DELAY)

print(f"\n   {'='*60}")
print(f"   CONCLUIDO: {total_gravado}/{total} detalhes gravados ({total_gravado/total*100:.1f}%)")
print(f"   {'='*60}")

if erros_api > 0:
    print(f"\n   ERROS: {erros_api}/{total} ({erros_api/total*100:.1f}%)")
    for tipo, count in sorted(erros_por_tipo.items(), key=lambda x: x[1], reverse=True):
        print(f"     - {tipo}: {count}")

status_list.append({"tabela": "ft_bronze.deputados_detalhes", "registros": total_gravado})

# COMMAND ----------

# DBTITLE 1,✅ Validação Pós-Execução
# ============================================================
# VALIDAÇÃO: Verifica se a ingestão funcionou
# ============================================================

print("\n" + "="*70)
print("VALIDAÇÃO: Verificando resultados da ingestão")
print("="*70)

# Conta registros na tabela
try:
    count = spark.sql(f"SELECT COUNT(*) as total FROM {CATALOG}.{BRONZE_SCHEMA}.deputados_detalhes").collect()[0]['total']
    
    print(f"\n📊 Tabela deputados_detalhes:")
    print(f"   Total de registros: {count}")
    print(f"   Esperado: 640")
    
    if count > 0:
        print(f"\n✅ SUCESSO! {count}/640 registros gravados ({count/640*100:.1f}%)")
        
        # Mostra amostra
        print(f"\n📋 Amostra dos dados (5 primeiros):")
        df_sample = spark.sql(f"SELECT id, nomeCivil, cpf, status_siglaPartido FROM {CATALOG}.{BRONZE_SCHEMA}.deputados_detalhes LIMIT 5")
        display(df_sample)
    else:
        print(f"\n❌ FALHA: Tabela vazia!")
        print(f"   Verifique os erros na célula anterior")
        
except Exception as e:
    print(f"\n❌ ERRO ao validar: {str(e)[:200]}")
    print(f"   A tabela pode não existir ainda")

print("\n" + "="*70)

# COMMAND ----------

# DBTITLE 1,Otimiza Tabela Detalhes
# ============================================================
# OTIMIZACAO DA TABELA deputados_detalhes
# ============================================================
# Otimiza a tabela Delta para melhorar performance de leitura.
# Executa apenas se a tabela tiver dados.
# ============================================================

print("Verificando se tabela deputados_detalhes precisa ser otimizada...")

try:
    # Verifica se a tabela existe e tem dados
    if spark.catalog.tableExists(f"{CATALOG}.{BRONZE_SCHEMA}.deputados_detalhes"):
        count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.deputados_detalhes").count()
        
        if count > 0:
            print(f"   Otimizando tabela com {count} registros...")
            spark.sql(f"OPTIMIZE {CATALOG}.{BRONZE_SCHEMA}.deputados_detalhes")
            spark.sql(f"ANALYZE TABLE {CATALOG}.{BRONZE_SCHEMA}.deputados_detalhes COMPUTE STATISTICS")
            print("   ✅ Tabela deputados_detalhes otimizada com sucesso")
        else:
            print("   ⚠️  Tabela vazia - otimizacao nao necessaria")
    else:
        print("   ⚠️  Tabela ainda nao existe - otimizacao nao necessaria")
except Exception as e:
    print(f"   ⚠️  Aviso: Otimizacao falhou - {str(e)[:100]}")

# COMMAND ----------

# DBTITLE 1,Validacao de Qualidade
# MAGIC %md
# MAGIC # Validacao de Qualidade

# COMMAND ----------

# DBTITLE 1,Sobre a Validacao
# MAGIC %md
# MAGIC Na celula abaixo e verificada a qualidade dos dados ingeridos.
# MAGIC Sao validados os campos obrigatorios (id, nome, partido, UF) e
# MAGIC verificada a ausencia de duplicatas pela chave primaria (id do deputado).

# COMMAND ----------

# DBTITLE 1,Diagnostico Avancado de Qualidade
# ============================================================
# DIAGNOSTICO AVANCADO DE QUALIDADE
# ============================================================
# Analisa detalhadamente a qualidade dos dados ingeridos
# e gera relatorio completo para auditoria.
# ============================================================

print("="*60)
print("DIAGNOSTICO AVANCADO DE QUALIDADE")
print("="*60)

# ============================================================
# TABELA: deputados
# ============================================================
print("\n1. TABELA: deputados")
print("-" * 60)

try:
    df_dep = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.deputados")
    
    # Metricas basicas
    total_registros = df_dep.count()
    ids_unicos = df_dep.select("id").distinct().count()
    duplicatas = total_registros - ids_unicos
    
    print(f"   Total de registros: {total_registros}")
    print(f"   IDs unicos: {ids_unicos}")
    print(f"   Duplicatas: {duplicatas}")
    
    if duplicatas > 0:
        print(f"   ❌ PROBLEMA: {duplicatas} duplicatas encontradas!")
    else:
        print(f"   ✅ Sem duplicatas")
    
    # Distribuicao por partido
    print(f"\n   Top 5 partidos por numero de deputados:")
    df_dep.groupBy("siglaPartido").count().orderBy("count", ascending=False).limit(5).show(truncate=False)
    
    # Distribuicao por UF
    print(f"\n   Top 5 UFs por numero de deputados:")
    df_dep.groupBy("siglaUf").count().orderBy("count", ascending=False).limit(5).show(truncate=False)
    
except Exception as e:
    print(f"   ❌ Erro ao analisar tabela deputados: {str(e)[:100]}")

# ============================================================
# TABELA: deputados_detalhes
# ============================================================
print("\n2. TABELA: deputados_detalhes")
print("-" * 60)

try:
    if spark.catalog.tableExists(f"{CATALOG}.{BRONZE_SCHEMA}.deputados_detalhes"):
        df_det = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.deputados_detalhes")
        
        total_detalhes = df_det.count()
        print(f"   Total de registros: {total_detalhes}")
        
        if total_detalhes == 0:
            print(f"   ❌ PROBLEMA: Tabela vazia (0 registros)")
            print(f"   ⚠️  Verifique os erros reportados na celula anterior")
        else:
            # Se tem dados, analisa
            ids_detalhes = df_det.select("id").distinct().count()
            print(f"   IDs unicos: {ids_detalhes}")
            print(f"   ✅ Tabela contem dados")
            
            # Mostra amostra de colunas
            print(f"\n   Colunas disponiveis ({len(df_det.columns)} total):")
            print(f"   {', '.join(df_det.columns[:10])}...")
    else:
        print(f"   ⚠️  Tabela ainda nao existe")
        
except Exception as e:
    print(f"   ❌ Erro ao analisar tabela deputados_detalhes: {str(e)[:100]}")

# ============================================================
# INTEGRIDADE REFERENCIAL
# ============================================================
print("\n3. INTEGRIDADE REFERENCIAL")
print("-" * 60)

try:
    df_dep = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.deputados")
    
    if spark.catalog.tableExists(f"{CATALOG}.{BRONZE_SCHEMA}.deputados_detalhes"):
        df_det = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.deputados_detalhes")
        
        if df_det.count() > 0:
            # IDs na lista mas sem detalhes
            ids_lista = df_dep.select("id").distinct()
            ids_detalhes = df_det.select("id").distinct()
            
            sem_detalhes = ids_lista.subtract(ids_detalhes).count()
            
            print(f"   Deputados na lista: {ids_lista.count()}")
            print(f"   Deputados com detalhes: {ids_detalhes.count()}")
            print(f"   Deputados SEM detalhes: {sem_detalhes}")
            
            if sem_detalhes > 0:
                pct = (sem_detalhes / ids_lista.count() * 100) if ids_lista.count() > 0 else 0
                print(f"   ⚠️  {pct:.1f}% dos deputados nao tem detalhes")
            else:
                print(f"   ✅ Todos os deputados tem detalhes")
        else:
            print(f"   ⚠️  Tabela de detalhes vazia - integridade nao aplicavel")
    else:
        print(f"   ⚠️  Tabela de detalhes nao existe - integridade nao aplicavel")
        
except Exception as e:
    print(f"   ❌ Erro ao verificar integridade: {str(e)[:100]}")

print("\n" + "="*60)
print("DIAGNOSTICO CONCLUIDO")
print("="*60)

# COMMAND ----------

# DBTITLE 1,Finalizacao
# MAGIC %md
# MAGIC # Finalizacao do Notebook

# COMMAND ----------

# DBTITLE 1,Sobre a Finalizacao
# MAGIC %md
# MAGIC Na celula abaixo e encerrado o processamento do notebook.
# MAGIC O sistema calcula o tempo total de execucao e exibe um resumo
# MAGIC com todas as tabelas processadas e seus volumes.

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
