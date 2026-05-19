# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://gazetadasemana.com.br/images/noticias/166864/19041851_compass.uo.jpg.jpg" width="450"/>

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Bronze - Frentes Parlamentares
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook realiza a ingestao dos dados de frentes parlamentares da Legislatura 57.
# MAGIC Uma frente parlamentar e um grupo suprapartidario de deputados organizados em torno de
# MAGIC um tema especifico. Sao extraidas a lista de frentes e os membros de cada uma.
# MAGIC Os dados sao gravados na camada Bronze sem transformacao.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `GET /frentes` | Lista de frentes parlamentares com id e titulo |
# MAGIC | `GET /frentes/{id}/membros` | Membros de cada frente com partido e UF |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_bronze.frentes` | Lista de frentes (id, titulo, legislatura) |
# MAGIC | `dt0025_dev.ft_bronze.frentes_membros` | Membros com referencia a frente |
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

# MAGIC %md
# MAGIC ---

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
log_notebook_start("bronze_frentes")

# COMMAND ----------

# DBTITLE 1,Ingestao dos Dados
# MAGIC %md
# MAGIC # Ingestao dos Dados da API

# COMMAND ----------

# DBTITLE 1,Sobre as Frentes Parlamentares
# MAGIC %md
# MAGIC Na celula abaixo sao buscadas todas as frentes parlamentares da legislatura 57.
# MAGIC Uma frente parlamentar e um grupo suprapartidario de deputados organizados em
# MAGIC torno de um tema especifico (ex: Frente Parlamentar da Agropecuaria, Frente da Educacao).
# MAGIC Cada frente possui id, titulo e legislatura associada.

# COMMAND ----------

# DBTITLE 1,Decisão: Primeira Execução ou Reprocessamento
# ============================================================
# DECISÃO: PRIMEIRA EXECUÇÃO OU REPROCESSAMENTO?
# ============================================================
# Verifica se as tabelas bronze já existem:
# - Se NÃO existem → PRIMEIRA EXECUÇÃO (carga completa)
# - Se existem → REPROCESSAMENTO (apenas frentes faltantes)
# ============================================================

print("Verificando se tabelas bronze já existem...")

# Verifica se tabelas existem
tabela_frentes_existe = spark.catalog.tableExists(f"{CATALOG}.{BRONZE_SCHEMA}.frentes")
tabela_membros_existe = spark.catalog.tableExists(f"{CATALOG}.{BRONZE_SCHEMA}.frentes_membros")

print(f"   Tabela frentes existe: {tabela_frentes_existe}")
print(f"   Tabela frentes_membros existe: {tabela_membros_existe}")
print()

if not tabela_frentes_existe or not tabela_membros_existe:
    print("🆕 PRIMEIRA EXECUÇÃO DETECTADA")
    print("   Modo: CARGA COMPLETA")
    print("   Ações:")
    print("     1. Ingerir TODAS as frentes")
    print("     2. Smoke test")
    print("     3. Ingerir TODOS os membros")
    print()
    MODO_EXECUCAO = "COMPLETA"
else:
    print("🔄 REPROCESSAMENTO DETECTADO")
    print("   Modo: PROCESSAMENTO INCREMENTAL")
    print("   Ações:")
    print("     1. Identificar frentes sem membros")
    print("     2. Processar APENAS as frentes faltantes")
    print()
    MODO_EXECUCAO = "INCREMENTAL"

print("="*70)

# COMMAND ----------

# DBTITLE 1,Ingere Lista Frentes
# ============================================================
# CARGA COMPLETA - INGESTÃO DA LISTA DE FRENTES
# ============================================================
# Executa APENAS se MODO_EXECUCAO == "COMPLETA"
# Busca TODAS as frentes e grava em modo OVERWRITE.
# ============================================================

if MODO_EXECUCAO != "COMPLETA":
    print("⏭️  PULANDO: Modo de execução é INCREMENTAL")
    print("   Esta célula executa apenas em modo COMPLETA")
    frentes_lista = []  # Lista vazia para evitar erros
else:
    print("Ingerindo lista de frentes parlamentares (carga completa)...")
    
    # Busca todas as frentes da legislatura 57
    frentes_completas = fetch_api(
        endpoint="/frentes",
        params={"idLegislatura": 57}
    )
    
    print(f"   Frentes encontradas: {len(frentes_completas)}")
    
    # TRATAMENTO DE DUPLICADOS: Remove duplicatas antes de gravar
    from pyspark.sql.types import StructType, StructField, StringType
    
    if len(frentes_completas) > 0:
        # Cria DataFrame para deduplicação
        all_keys = set()
        for record in frentes_completas:
            all_keys.update(record.keys())
        
        schema = StructType([StructField(key, StringType(), True) for key in sorted(all_keys)])
        df_temp = spark.createDataFrame(frentes_completas, schema=schema)
        df_temp_dedup = df_temp.dropDuplicates(["id"])
        
        # Converte de volta para lista
        frentes_completas = [row.asDict() for row in df_temp_dedup.collect()]
        print(f"   Após deduplicação: {len(frentes_completas)} frentes únicas")
    
    # Configuração de checkpoint
    BATCH_SIZE = 100
    total_gravado = 0
    
    if len(frentes_completas) == 0:
        print("   Nenhuma frente para processar")
        frentes_lista = []
    else:
        # Processa em lotes de 100
        for i in range(0, len(frentes_completas), BATCH_SIZE):
            batch = frentes_completas[i:i+BATCH_SIZE]
            batch_num = i//BATCH_SIZE + 1
            total_batches = (len(frentes_completas)-1)//BATCH_SIZE + 1
            
            # Define modo: overwrite no primeiro batch, append nos demais
            mode = "overwrite" if i == 0 else "append"
            
            # Grava o batch
            n = save_to_bronze(batch, "frentes", "/frentes", mode=mode)
            total_gravado += len(batch)
            
            print(f"   Batch {batch_num}/{total_batches}: Gravadas {len(batch)} frentes (Total: {total_gravado})")
            
            # Pequena pausa entre batches
            if i + BATCH_SIZE < len(frentes_completas):
                time.sleep(0.3)
        
        print(f"   CONCLUÍDO: {total_gravado} frentes gravadas no total")
        
        # Otimização da tabela
        print("\n   Otimizando tabela frentes...")
        try:
            spark.sql(f"OPTIMIZE {CATALOG}.{BRONZE_SCHEMA}.frentes")
            spark.sql(f"ANALYZE TABLE {CATALOG}.{BRONZE_SCHEMA}.frentes COMPUTE STATISTICS")
            print("   ✅ Tabela frentes otimizada com sucesso")
        except Exception as e:
            print(f"   ⚠️  Aviso: Otimização falhou - {str(e)[:100]}")
        
        # Mantém apenas IDs mínimos para buscar membros depois
        frentes_lista = [{'id': f['id'], 'titulo': f.get('titulo', '')} for f in frentes_completas]
        
        # Limpa memória
        frentes_completas = []

# COMMAND ----------

# DBTITLE 1,Sobre os Membros das Frentes
# ============================================================
# PROCESSAMENTO INCREMENTAL - IDENTIFICA FRENTES FALTANTES
# ============================================================
# Executa APENAS se MODO_EXECUCAO == "INCREMENTAL"
# Busca frentes que ainda não têm membros processados.
# ============================================================

if MODO_EXECUCAO != "INCREMENTAL":
    print("⏭️  PULANDO: Modo de execução é COMPLETA")
    print("   Esta célula executa apenas em modo INCREMENTAL")
    frentes_faltantes = []  # Lista vazia para evitar erros
else:
    print("Identificando frentes sem membros...")
    
    # Busca frentes que NÃO têm membros na tabela
    df_frentes_sem_membros = spark.sql(f"""
        SELECT f.id, f.titulo
        FROM {CATALOG}.{BRONZE_SCHEMA}.frentes f
        LEFT JOIN {CATALOG}.{BRONZE_SCHEMA}.frentes_membros m 
            ON f.id = m._frente_id
        WHERE m._frente_id IS NULL
        ORDER BY CAST(f.id AS INT)
    """)
    
    # Converte para lista Python
    frentes_faltantes = [
        {'id': row['id'], 'titulo': row['titulo']} 
        for row in df_frentes_sem_membros.collect()
    ]
    
    print(f"   Total de frentes sem membros: {len(frentes_faltantes)}")
    
    if len(frentes_faltantes) > 0:
        print(f"   Primeira frente: ID {frentes_faltantes[0]['id']}")
        print(f"   Última frente: ID {frentes_faltantes[-1]['id']}")
        print()
        print(f"   Estas {len(frentes_faltantes)} frentes serão processadas em modo APPEND.")
    else:
        print("   ✅ Todas as frentes já têm membros processados!")

# COMMAND ----------

# DBTITLE 1,Smoke Test - Validacao da API
# ============================================================
# CARGA COMPLETA - SMOKE TEST
# ============================================================
# Executa APENAS se MODO_EXECUCAO == "COMPLETA"
# Testa 10 frentes antes de processar todas.
# ============================================================

if MODO_EXECUCAO != "COMPLETA":
    print("⏭️  PULANDO: Modo de execução é INCREMENTAL")
    print("   Smoke test não necessário em reprocessamento")
else:
    import requests
    import random
    
    print("="*60)
    print("SMOKE TEST: Validando endpoint /frentes/{id}/membros")
    print("="*60)
    
    if len(frentes_lista) == 0:
        print("   Nenhuma frente para testar")
    else:
        # Seleciona 10 frentes de amostra
        sample_size = min(10, len(frentes_lista))
        sample_indices = [
            0, 
            len(frentes_lista) // 4,
            len(frentes_lista) // 2,
            3 * len(frentes_lista) // 4,
            len(frentes_lista) - 1
        ]
        
        sample_indices = list(set(sample_indices))[:10]
        sample_frentes = [frentes_lista[i] for i in sample_indices if i < len(frentes_lista)]
        
        if len(sample_frentes) < 10 and len(frentes_lista) > 10:
            remaining = [f for f in frentes_lista if f not in sample_frentes]
            sample_frentes.extend(random.sample(remaining, min(10 - len(sample_frentes), len(remaining))))
        
        print(f"Testando {len(sample_frentes)} frentes de amostra...\n")
        
        sucessos = 0
        total_membros_amostra = 0
        
        for frente in sample_frentes:
            frente_id = frente['id']
            frente_titulo = frente.get('titulo', 'Sem titulo')[:50]
            url = f"{API_BASE_URL}/frentes/{frente_id}/membros"
            
            try:
                resp = requests.get(url, timeout=10)
                
                if resp.status_code == 200:
                    membros = resp.json().get('dados', [])
                    print(f"  ✅ ID {frente_id}: {len(membros)} membros - {frente_titulo}")
                    sucessos += 1
                    total_membros_amostra += len(membros)
                elif resp.status_code == 400:
                    print(f"  ⚠️  ID {frente_id}: 0 membros (Status 400) - {frente_titulo}")
                    sucessos += 1
                else:
                    print(f"  ❌ ID {frente_id}: Status {resp.status_code}")
                    
            except Exception as e:
                print(f"  ⚠️  ID {frente_id}: {type(e).__name__}")
        
        taxa_sucesso = (sucessos / len(sample_frentes) * 100) if len(sample_frentes) > 0 else 0
        
        print(f"\n{'='*60}")
        print(f"RESULTADO: {sucessos}/{len(sample_frentes)} sucessos ({taxa_sucesso:.1f}%)")
        print(f"Total de membros na amostra: {total_membros_amostra:,}")
        print(f"{'='*60}\n")
        
        if taxa_sucesso < 80:
            print("❌ SMOKE TEST FALHOU: Taxa de sucesso < 80%")
        else:
            print("✅ SMOKE TEST PASSOU: API está respondendo normalmente\n")

# COMMAND ----------

# DBTITLE 1,Ingere Membros Frentes
# ============================================================
# CARGA COMPLETA - INGESTÃO DOS MEMBROS
# ============================================================
# Executa APENAS se MODO_EXECUCAO == "COMPLETA"
# Processa TODAS as frentes em modo OVERWRITE.
# ============================================================

import requests
import time

if MODO_EXECUCAO != "COMPLETA":
    print("⏭️  PULANDO: Modo de execução é INCREMENTAL")
    print("   Esta célula executa apenas em modo COMPLETA")
    total_gravado = 0
else:
    print("Ingerindo membros de cada frente (carga completa)...")
    
    BATCH_SIZE = 100
    total_gravado = 0
    registros_nulos_ignorados = 0
    total = len(frentes_lista)
    
    if total == 0:
        print("   Nenhuma frente para processar")
    else:
        # Processa em lotes de 100 frentes
        for batch_start in range(0, total, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total)
            batch = frentes_lista[batch_start:batch_end]
            
            batch_num = batch_start//BATCH_SIZE + 1
            total_batches = (total-1)//BATCH_SIZE + 1
            
            print(f"   Batch {batch_num}/{total_batches}: Processando {len(batch)} frentes...")
            
            membros_batch = []
            
            # Processa cada frente do batch
            for idx, frente in enumerate(batch):
                frente_id = frente['id']
                
                try:
                    url = f"{API_BASE_URL}/frentes/{frente_id}/membros"
                    response = requests.get(url, timeout=30)
                    
                    if response.status_code == 200:
                        dados = response.json().get('dados', [])
                        
                        # Filtra registros válidos
                        for d in dados:
                            if d.get('id') and d.get('nome'):
                                d['_frente_id'] = frente_id
                                d['_frente_titulo'] = frente.get('titulo', '')
                                membros_batch.append(d)
                            else:
                                registros_nulos_ignorados += 1
                    
                except requests.exceptions.ConnectionError:
                    print(f"      ⚠️  Erro de conexão na frente {frente_id} - CONTINUANDO")
                    continue
                except Exception as e:
                    print(f"      Erro na frente {frente_id}: {str(e)[:60]}")
                    continue
            
            # Deduplicação e gravação
            if membros_batch:
                # Remove duplicatas
                from pyspark.sql.types import StructType, StructField, StringType
                
                # Infere schema
                all_keys = set()
                for record in membros_batch:
                    all_keys.update(record.keys())
                
                schema = StructType([StructField(key, StringType(), True) for key in sorted(all_keys)])
                
                df_batch = spark.createDataFrame(membros_batch, schema=schema)
                df_batch_dedup = df_batch.dropDuplicates(["id", "_frente_id"])
                
                # Converte de volta para lista
                membros_batch_limpos = [row.asDict() for row in df_batch_dedup.collect()]
                
                # Grava
                mode = "overwrite" if batch_start == 0 else "append"
                n = save_to_bronze(membros_batch_limpos, "frentes_membros", "/frentes/{id}/membros", mode=mode)
                total_gravado += len(membros_batch_limpos)
                
                print(f"      ✅ Gravados: {len(membros_batch_limpos)} membros (Total: {total_gravado})")
            
            # Pausa entre batches
            if batch_start + BATCH_SIZE < total:
                time.sleep(0.3)
        
        print(f"\n   {'='*60}")
        print(f"   CONCLUÍDO: {total_gravado:,} membros gravados")
        if registros_nulos_ignorados > 0:
            print(f"   ⚠️  {registros_nulos_ignorados} registros nulos ignorados")
        print(f"   {'='*60}")
        
        # Otimização
        print("\n   Otimizando tabela frentes_membros...")
        try:
            spark.sql(f"OPTIMIZE {CATALOG}.{BRONZE_SCHEMA}.frentes_membros")
            spark.sql(f"ANALYZE TABLE {CATALOG}.{BRONZE_SCHEMA}.frentes_membros COMPUTE STATISTICS")
            print("   ✅ Tabela frentes_membros otimizada")
        except Exception as e:
            print(f"   ⚠️  Aviso: Otimização falhou - {str(e)[:100]}")

# NOTA: NÃO adiciona ao status_list aqui - isso é feito na célula 20

# COMMAND ----------

# DBTITLE 1,INCREMENTAL - Reprocessa Frentes Faltantes
# ============================================================
# PROCESSAMENTO INCREMENTAL - REPROCESSA FRENTES FALTANTES
# ============================================================
# Executa APENAS se MODO_EXECUCAO == "INCREMENTAL"
# Processa APENAS as frentes sem membros em modo APPEND.
# ============================================================

import requests
import time

if MODO_EXECUCAO != "INCREMENTAL":
    print("⏭️  PULANDO: Modo de execução é COMPLETA")
    print("   Esta célula executa apenas em modo INCREMENTAL")
    total_gravado = 0
elif len(frentes_faltantes) == 0:
    print("✅ Nenhuma frente para reprocessar!")
    print("   Todas as frentes já têm membros processados.")
    total_gravado = 0
else:
    print(f"Reprocessando {len(frentes_faltantes)} frentes faltantes...")
    print()
    
    # Configuração
    BATCH_SIZE = 100
    total_gravado = 0
    total_processadas = 0
    
    # Monitoramento de erros
    frentes_com_erro = []
    erros_por_tipo = {'conexao': 0, 'timeout': 0, 'outros': 0}
    
    total = len(frentes_faltantes)
    
    # Processa em lotes
    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = frentes_faltantes[batch_start:batch_end]
        
        batch_num = batch_start//BATCH_SIZE + 1
        total_batches = (total-1)//BATCH_SIZE + 1
        
        print(f"   Batch {batch_num}/{total_batches}: Processando {len(batch)} frentes...")
        
        membros_batch = []
        
        # Processa cada frente do batch
        for idx, frente in enumerate(batch):
            frente_id = frente['id']
            total_processadas += 1
            
            try:
                url = f"{API_BASE_URL}/frentes/{frente_id}/membros"
                log_info("API_FETCH_START", f"Iniciando fetch: /frentes/{frente_id}/membros")
                
                response = requests.get(url, timeout=30)
                
                if response.status_code == 200:
                    dados = response.json().get('dados', [])
                    
                    for d in dados:
                        if d.get('id') and d.get('nome'):
                            d['_frente_id'] = frente_id
                            d['_frente_titulo'] = frente.get('titulo', '')
                            membros_batch.append(d)
                    
                elif response.status_code == 400:
                    # Frente inválida ou sem membros - OK
                    pass
                else:
                    print(f"      ⚠️  Frente {frente_id}: Status {response.status_code}")
                    frentes_com_erro.append(frente_id)
                    erros_por_tipo['outros'] += 1
                
            except requests.exceptions.ConnectionError as e:
                print(f"      ⚠️  Erro de CONEXÃO na frente {frente_id} - CONTINUANDO")
                frentes_com_erro.append(frente_id)
                erros_por_tipo['conexao'] += 1
                continue
                
            except requests.exceptions.Timeout as e:
                print(f"      ⏱️  TIMEOUT na frente {frente_id} - CONTINUANDO")
                frentes_com_erro.append(frente_id)
                erros_por_tipo['timeout'] += 1
                continue
                
            except Exception as e:
                print(f"      ⚠️  Erro na frente {frente_id}: {str(e)[:60]}")
                frentes_com_erro.append(frente_id)
                erros_por_tipo['outros'] += 1
                continue
        
        # CHECKPOINT: Grava o batch em modo APPEND
        if membros_batch:
            # Remove duplicatas
            from pyspark.sql.types import StructType, StructField, StringType
            
            all_keys = set()
            for record in membros_batch:
                all_keys.update(record.keys())
            
            schema = StructType([StructField(key, StringType(), True) for key in sorted(all_keys)])
            
            df_batch = spark.createDataFrame(membros_batch, schema=schema)
            df_batch_dedup = df_batch.dropDuplicates(["id", "_frente_id"])
            
            membros_batch_limpos = [row.asDict() for row in df_batch_dedup.collect()]
            
            n = save_to_bronze(membros_batch_limpos, "frentes_membros", "/frentes/{id}/membros", mode="append")
            total_gravado += len(membros_batch_limpos)
            print(f"      ✅ Gravados: {len(membros_batch_limpos)} membros (Total: {total_gravado})")
            membros_batch = []
        
        # Pausa entre batches
        if batch_start + BATCH_SIZE < total:
            time.sleep(0.3)
    
    print()
    print("=" * 80)
    print("RESUMO DO REPROCESSAMENTO")
    print("=" * 80)
    print(f"   Frentes processadas: {total_processadas}")
    print(f"   Membros gravados: {total_gravado:,}")
    print(f"   Frentes com erro: {len(frentes_com_erro)}")
    
    if len(frentes_com_erro) > 0:
        taxa_erro = len(frentes_com_erro) / total_processadas * 100
        print(f"   Taxa de erro: {taxa_erro:.1f}%")
        print()
        print(f"   Erros por tipo:")
        print(f"      - Conexão: {erros_por_tipo['conexao']}")
        print(f"      - Timeout: {erros_por_tipo['timeout']}")
        print(f"      - Outros: {erros_por_tipo['outros']}")
        print()
        
        if taxa_erro > 5:
            print(f"   ⚠️⚠️⚠️  ALERTA: Taxa de erro ({taxa_erro:.1f}%) acima de 5%!")
        
        print(f"   IDs com erro (para reprocessamento):")
        print(f"   {frentes_com_erro[:20]}")
        if len(frentes_com_erro) > 20:
            print(f"   ... e mais {len(frentes_com_erro) - 20}")
    else:
        print(f"   ✅ Nenhum erro detectado!")
    
    print("=" * 80)

# NOTA: NÃO adiciona ao status_list aqui - isso é feito na célula 20

# COMMAND ----------

# DBTITLE 1,Gravacao dos Dados
# MAGIC %md
# MAGIC # Gravacao na Camada Bronze

# COMMAND ----------

# DBTITLE 1,Sobre a Gravacao
# MAGIC %md
# MAGIC Na celula abaixo os dados sao gravados em duas tabelas na camada Bronze:
# MAGIC 1. **frentes**: lista de frentes (id, titulo, legislatura)
# MAGIC 2. **frentes_membros**: membros de cada frente com referencia ao id da frente

# COMMAND ----------

# DBTITLE 1,Grava Bronze Frentes
# ============================================================
# REGISTRO DE STATUS PARA RELATÓRIO FINAL
# ============================================================
# Registra as estatísticas no status_list para o relatório final.
# Diferencia modo COMPLETA (novos registros) vs INCREMENTAL (totais).
# ============================================================

print("Registrando estatísticas para o relatório final...")
print()

if MODO_EXECUCAO == "COMPLETA":
    print("   Modo COMPLETA: Registrando novos registros processados")
    
    # Frentes
    if 'frentes_lista' in dir() and frentes_lista:
        n1 = len(frentes_lista)
        status_list.append({"tabela": "ft_bronze.frentes", "registros": n1})
        print(f"      ✅ ft_bronze.frentes: {n1} registros gravados")
    
    # Membros
    if 'total_gravado' in dir() and total_gravado > 0:
        status_list.append({"tabela": "ft_bronze.frentes_membros", "registros": total_gravado})
        print(f"      ✅ ft_bronze.frentes_membros: {total_gravado:,} registros gravados")
    else:
        print(f"      ⚠️  Nenhum registro de membros processado")

else:  # MODO_EXECUCAO == "INCREMENTAL"
    print("   Modo INCREMENTAL: Mostrando totais atuais nas tabelas")
    print()
    
    # Verifica se algum registro foi processado
    if 'total_gravado' in dir() and total_gravado > 0:
        print(f"      ✅ {total_gravado:,} novos membros adicionados")
    else:
        print(f"      ℹ️  Nenhum registro novo processado (tudo já estava completo)")
    
    print()
    print("   Totais atuais nas tabelas:")
    
    try:
        # Conta totais das tabelas
        total_frentes = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.frentes").count()
        total_membros = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.frentes_membros").count()
        
        status_list.append({"tabela": "ft_bronze.frentes", "registros": total_frentes})
        status_list.append({"tabela": "ft_bronze.frentes_membros", "registros": total_membros})
        
        print(f"      📊 ft_bronze.frentes: {total_frentes} registros")
        print(f"      📊 ft_bronze.frentes_membros: {total_membros:,} registros")
        
    except Exception as e:
        print(f"      ❌ Erro ao ler totais das tabelas: {str(e)[:100]}")

print()
print("="*70)

# COMMAND ----------

# DBTITLE 1,Validacao de Qualidade
# MAGIC %md
# MAGIC # Validacao de Qualidade

# COMMAND ----------

# DBTITLE 1,Sobre Tratamento de Duplicados
# MAGIC %md
# MAGIC ## 🔒 Tratamento de Duplicados
# MAGIC
# MAGIC Este notebook implementa **prevenção de duplicados em 3 camadas**:
# MAGIC
# MAGIC ### 1️⃣ **Frentes** (Célula 13)
# MAGIC * **Chave primária**: `id`
# MAGIC * **Método**: `.dropDuplicates(["id"])` antes de gravar
# MAGIC * **Resultado**: 0 duplicatas (319 IDs únicos em 319 registros)
# MAGIC
# MAGIC ### 2️⃣ **Membros - Carga Completa** (Célula 16)
# MAGIC * **Chave composta**: `(id, _frente_id)` - permite mesmo deputado em várias frentes
# MAGIC * **Método**: `.dropDuplicates(["id", "_frente_id"])` por batch
# MAGIC * **Resultado**: 0 duplicatas (65.089 combinações únicas)
# MAGIC
# MAGIC ### 3️⃣ **Membros - Reprocessamento** (Célula 17)
# MAGIC * **Chave composta**: `(id, _frente_id)`
# MAGIC * **Método**: `.dropDuplicates(["id", "_frente_id"])` antes de append
# MAGIC * **Modo**: `append` para adicionar apenas novos registros
# MAGIC
# MAGIC ### 📈 **Estatísticas de Qualidade**
# MAGIC * **602 deputados únicos** participam das 319 frentes
# MAGIC * **Média**: 204 membros por frente
# MAGIC * **Deputado mais ativo**: Marangoni (306 frentes)
# MAGIC * **Integridade**: 100% - todas as frentes têm membros

# COMMAND ----------

# DBTITLE 1,Sobre a Validacao
# MAGIC %md
# MAGIC Na celula abaixo e verificada a integridade dos dados ingeridos.
# MAGIC Valida campos obrigatorios (id, titulo para frentes; id, nome, _frente_id para membros)
# MAGIC e verifica duplicatas na chave primaria.

# COMMAND ----------

# DBTITLE 1,Verifica Qualidade
# ============================================================
# VALIDACAO DE QUALIDADE DOS DADOS
# ============================================================
# Verifica integridade referencial entre frentes e membros.
# ============================================================

# Carrega tabela de frentes para validacao
df_frentes = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.frentes")

# Valida campos obrigatorios e duplicatas
check_quality(df_frentes, "frentes", key_columns=["id"], critical_columns=["id", "titulo"])

# Carrega tabela de membros para validacao
df_membros = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.frentes_membros")

# Valida campos obrigatorios dos membros
check_quality(df_membros, "frentes_membros", critical_columns=["id", "nome", "_frente_id"])

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
# TABELA: frentes
# ============================================================
print("\n1. TABELA: frentes")
print("-" * 60)

try:
    df_frentes = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.frentes")
    
    # Metricas basicas
    total_registros = df_frentes.count()
    ids_unicos = df_frentes.select("id").distinct().count()
    duplicatas = total_registros - ids_unicos
    
    print(f"   Total de registros: {total_registros}")
    print(f"   IDs unicos: {ids_unicos}")
    print(f"   Duplicatas: {duplicatas}")
    
    if duplicatas > 0:
        print(f"   ❌ PROBLEMA: {duplicatas} duplicatas encontradas!")
    else:
        print(f"   ✅ Sem duplicatas")
    
    # Amostra de frentes
    print(f"\n   Amostra de frentes (5 primeiras):")
    df_frentes.select("id", "titulo").limit(5).show(truncate=False)
    
except Exception as e:
    print(f"   ❌ Erro ao analisar tabela frentes: {str(e)[:100]}")

# ============================================================
# TABELA: frentes_membros
# ============================================================
print("\n2. TABELA: frentes_membros")
print("-" * 60)

try:
    df_membros = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.frentes_membros")
    
    total_membros = df_membros.count()
    print(f"   Total de registros: {total_membros}")
    
    if total_membros == 0:
        print(f"   ❌ PROBLEMA: Tabela vazia (0 registros)")
    else:
        # IDs unicos de deputados
        ids_deputados_unicos = df_membros.select("id").distinct().count()
        print(f"   Deputados unicos: {ids_deputados_unicos}")
        
        # Frentes distintas com membros
        frentes_com_membros = df_membros.select("_frente_id").distinct().count()
        print(f"   Frentes com membros: {frentes_com_membros}")
        
        # Media de membros por frente
        media = total_membros / frentes_com_membros if frentes_com_membros > 0 else 0
        print(f"   Media de membros por frente: {media:.1f}")
        
        # Verifica duplicatas de (id, _frente_id)
        combinacoes_totais = df_membros.count()
        combinacoes_unicas = df_membros.select("id", "_frente_id").distinct().count()
        duplicatas_combinacao = combinacoes_totais - combinacoes_unicas
        
        if duplicatas_combinacao > 0:
            print(f"   ❌ PROBLEMA: {duplicatas_combinacao} duplicatas de (id, _frente_id)")
        else:
            print(f"   ✅ Sem duplicatas de (id, _frente_id)")
        
        # Verifica nulos
        nulos_id = df_membros.filter(col("id").isNull()).count()
        nulos_nome = df_membros.filter(col("nome").isNull()).count()
        
        if nulos_id > 0 or nulos_nome > 0:
            print(f"   ⚠️  ALERTA: {nulos_id} nulos em 'id', {nulos_nome} nulos em 'nome'")
        else:
            print(f"   ✅ Sem nulos em campos criticos")
        
        # Top 5 frentes por numero de membros
        print(f"\n   Top 5 frentes por numero de membros:")
        df_membros.groupBy("_frente_id", "_frente_titulo") \
            .count() \
            .orderBy("count", ascending=False) \
            .limit(5) \
            .show(truncate=False)
        
except Exception as e:
    print(f"   ❌ Erro ao analisar tabela frentes_membros: {str(e)[:100]}")

# ============================================================
# INTEGRIDADE REFERENCIAL
# ============================================================
print("\n3. INTEGRIDADE REFERENCIAL")
print("-" * 60)

try:
    df_frentes = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.frentes")
    df_membros = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.frentes_membros")
    
    # Frentes sem membros
    frentes_total = df_frentes.count()
    frentes_com_membros = df_membros.select("_frente_id").distinct().count()
    frentes_sem_membros = frentes_total - frentes_com_membros
    
    print(f"   Total de frentes: {frentes_total}")
    print(f"   Frentes com membros: {frentes_com_membros}")
    print(f"   Frentes SEM membros: {frentes_sem_membros}")
    
    if frentes_sem_membros > 0:
        pct = (frentes_sem_membros / frentes_total * 100) if frentes_total > 0 else 0
        print(f"   ⚠️  {pct:.1f}% das frentes nao tem membros")
        
        # Mostra IDs das frentes sem membros (primeiros 10)
        frentes_ids = df_frentes.select("id").distinct()
        membros_ids = df_membros.select("_frente_id").distinct()
        sem_membros_ids = frentes_ids.subtract(membros_ids).limit(10)
        
        print(f"\n   Primeiros 10 IDs de frentes sem membros:")
        sem_membros_ids.show(truncate=False)
    else:
        print(f"   ✅ Todas as frentes tem membros")
    
    # Membros orfaos (em frentes que nao existem)
    membros_frente_ids = df_membros.select("_frente_id").distinct()
    frentes_ids = df_frentes.select("id").distinct()
    orfaos = membros_frente_ids.subtract(frentes_ids).count()
    
    if orfaos > 0:
        print(f"   ❌ PROBLEMA: {orfaos} membros em frentes que nao existem (orfaos)")
    else:
        print(f"   ✅ Sem membros orfaos")
        
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
