# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://gazetadasemana.com.br/images/noticias/166864/19041851_compass.uo.jpg.jpg" width="450"/>

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Bronze - Despesas CEAP
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook realiza a ingestao incremental das despesas CEAP (Cota para Exercicio da
# MAGIC Atividade Parlamentar) de cada deputado. Para cada deputado ativo, busca despesas por
# MAGIC ano e mes. O controle incremental e feito pelo ultimo ano/mes processado.
# MAGIC Cada despesa contem: tipo, valor, fornecedor, CNPJ/CPF, data e documento.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `GET /deputados/{id}/despesas` | Despesas por deputado, ano e mes |
# MAGIC | `dt0025_dev.ft_bronze.deputados` | Lista de IDs de deputados ativos |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_bronze.despesas` | Despesas CEAP de todos os deputados |
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
log_notebook_start("bronze_despesas")

# COMMAND ----------

# DBTITLE 1,Recuperacao de Deputados
# MAGIC %md
# MAGIC # Recuperacao da Lista de Deputados

# COMMAND ----------

# DBTITLE 1,Sobre a Lista de Deputados
# MAGIC %md
# MAGIC Na celula abaixo e recuperada a lista de IDs dos deputados ativos a partir da tabela
# MAGIC Bronze ja existente. Isso evita uma chamada extra a API. Caso a tabela nao exista
# MAGIC (primeira execucao), busca diretamente da API como fallback.

# COMMAND ----------

# DBTITLE 1,Busca Lista Deputados
# ============================================================
# RECUPERA LISTA DE DEPUTADOS ATIVOS
# ============================================================
# Le a tabela bronze de deputados para obter os IDs.
# Evita chamada extra a API reaproveitando dados ja ingeridos.
# ============================================================

# Informa o usuario
print("Recuperando lista de deputados...")

try:
    # Tenta ler a tabela de deputados ja existente na Bronze
    df_deps = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.deputados")
    
    # Extrai lista de IDs unicos de deputados
    dep_ids = [row['id'] for row in df_deps.select("id").distinct().collect()]
    
except Exception:
    # Se a tabela nao existe, busca direto da API (fallback)
    print("  Tabela deputados nao encontrada - buscando da API...")
    deps = fetch_api("/deputados", params={"idLegislatura": 57})
    dep_ids = [d['id'] for d in deps]

# Exibe quantidade de deputados encontrados
print(f"   Deputados: {len(dep_ids)}")

# COMMAND ----------

# DBTITLE 1,Ingestao Incremental
# MAGIC %md
# MAGIC # Ingestao Incremental de Despesas

# COMMAND ----------

# DBTITLE 1,Sobre a Ingestao por Ano/Mes
# MAGIC %md
# MAGIC Na celula abaixo, para cada deputado, sao buscadas as despesas organizadas por ano e mes.
# MAGIC A API da Camara exige esses filtros para retornar os dados. O watermark controla qual
# MAGIC foi o ultimo periodo processado, evitando reprocessamento desnecessario.
# MAGIC Cada despesa contem: tipo (passagem, alimentacao, etc), valor, fornecedor, CNPJ e data.

# COMMAND ----------

# DBTITLE 1,Ingere Despesas com Checkpoints
# ============================================================
# INGESTÃO INCREMENTAL DE DESPESAS POR DEPUTADO
# ============================================================
# Para cada deputado, busca despesas dos últimos meses.
# Cada despesa contém: tipoDespesa, valorDocumento,
# valorLiquido, fornecedor, CNPJ/CPF, data e documento.
# OTIMIZADO: Grava em batches periódicos com deduplicação
# ============================================================

print("Ingerindo despesas CEAP (versão otimizada com checkpoints)...")

# Verifica se é primeira execução
tabela_existe = spark.catalog.tableExists(f"{CATALOG}.{BRONZE_SCHEMA}.despesas")
print(f"   Tabela existe: {tabela_existe}")
print()

# Recupera watermark
watermark = get_watermark("despesas")

# Define período
if isinstance(watermark, dict) and watermark.get("last_date") and tabela_existe:
    # Incremental: processa apenas períodos novos
    wm_year = int(watermark["last_date"][:4])
    wm_month = int(watermark["last_date"][5:7])
    
    # Próximo mês após o watermark
    if wm_month == 12:
        anos = [wm_year + 1]
        meses = list(range(1, 13))
    else:
        anos = [wm_year]
        meses = list(range(wm_month + 1, 13))
        # Se já está em 2025, adiciona ano seguinte
        if wm_year < 2025:
            anos.append(wm_year + 1)
            meses.extend(range(1, 13))
    
    print(f"   ✅ Modo INCREMENTAL")
    print(f"   Watermark: {watermark['last_date']}")
    print(f"   Processando: anos {anos}, meses {meses}")
else:
    # Primeira execução: carga completa
    anos = [2023, 2024, 2025]
    meses = list(range(1, 13))
    print(f"   🆕 Modo CARGA COMPLETA")
    print(f"   Processando: anos {anos}")

print()

# Define MAX_WORKERS se não estiver definido
if 'MAX_WORKERS' not in dir():
    MAX_WORKERS = 5
    print(f"   MAX_WORKERS definido como {MAX_WORKERS}")

# Cria lista de todas as requisições
endpoints_to_fetch = []
for dep_id in dep_ids:
    for ano in anos:
        for mes in meses:
            endpoints_to_fetch.append((
                f"/deputados/{dep_id}/despesas",
                {"ano": ano, "mes": mes, "itens": 100}
            ))

print(f"   Total de requisições: {len(endpoints_to_fetch):,}")
print(f"   Processando em paralelo com {MAX_WORKERS} threads...")
print()

# Configurações de checkpoint
BATCH_SIZE = 500  # Requisições por batch paralelo
CHECKPOINT_SIZE = 10000  # Grava a cada 10k despesas
despesas_lista = []
total_gravado = 0
total_filtrados = 0  # Nulos/inválidos ignorados
erros_rede = 0

# Modo de gravação: overwrite na primeira vez, append depois
first_checkpoint = True

# Processa em batches
for batch_idx in range(0, len(endpoints_to_fetch), BATCH_SIZE):
    batch = endpoints_to_fetch[batch_idx:batch_idx+BATCH_SIZE]
    batch_num = batch_idx // BATCH_SIZE + 1
    total_batches = (len(endpoints_to_fetch) - 1) // BATCH_SIZE + 1
    
    print(f"   Batch {batch_num}/{total_batches}: {len(batch)} requisições")
    
    # Busca batch em paralelo
    batch_results = fetch_api_parallel(batch, max_workers=MAX_WORKERS)
    
    # Processa resultados
    for result in batch_results:
        if result['success']:
            dep_id = result['endpoint'].split('/')[2]
            for despesa in result['data']:
                # Validação básica
                if despesa.get('codDocumento') and despesa.get('tipoDespesa'):
                    despesa['_deputado_id'] = dep_id
                    despesas_lista.append(despesa)
                else:
                    total_filtrados += 1
        else:
            erros_rede += 1
    
    # CHECKPOINT: Grava se atingiu o tamanho configurado
    if len(despesas_lista) >= CHECKPOINT_SIZE:
        print(f"      💾 CHECKPOINT: Gravando {len(despesas_lista):,} despesas...")
        
        # ✅ DEDUPLICAÇÃO: Remove duplicados
        from pyspark.sql.types import StructType, StructField, StringType
        
        # Cria DataFrame
        all_keys = set()
        for record in despesas_lista:
            all_keys.update(record.keys())
        
        schema = StructType([StructField(key, StringType(), True) for key in sorted(all_keys)])
        df_batch = spark.createDataFrame(despesas_lista, schema=schema)
        
        # Remove duplicados pela chave primária
        df_batch_dedup = df_batch.dropDuplicates(["codDocumento", "_deputado_id"])
        despesas_limpas = [row.asDict() for row in df_batch_dedup.collect()]
        
        # Define modo
        if first_checkpoint and not tabela_existe:
            mode = "overwrite"
            first_checkpoint = False
            print(f"         Modo: OVERWRITE (primeira gravação)")
        else:
            mode = "append"
            print(f"         Modo: APPEND")
        
        # Grava
        save_to_bronze(despesas_limpas, "despesas", "/deputados/{id}/despesas", mode=mode)
        
        # Atualiza contadores
        duplicados_removidos = len(despesas_lista) - len(despesas_limpas)
        total_gravado += len(despesas_limpas)
        
        print(f"         ✅ Gravados: {len(despesas_limpas):,} | Duplicados removidos: {duplicados_removidos}")
        print(f"         📊 Total acumulado: {total_gravado:,} despesas")
        
        # Limpa lista para liberar memória
        despesas_lista = []
    else:
        # Exibe progresso
        print(f"      📝 Acumuladas: {len(despesas_lista):,} | Gravadas: {total_gravado:,} | Erros: {erros_rede}")
    
    # Pausa entre batches
    if batch_idx + BATCH_SIZE < len(endpoints_to_fetch):
        time.sleep(1)

# GRAVAÇÃO FINAL: Salva registros restantes
if despesas_lista:
    print(f"\n   💾 GRAVAÇÃO FINAL: {len(despesas_lista):,} despesas restantes...")
    
    # Deduplicação
    from pyspark.sql.types import StructType, StructField, StringType
    
    all_keys = set()
    for record in despesas_lista:
        all_keys.update(record.keys())
    
    schema = StructType([StructField(key, StringType(), True) for key in sorted(all_keys)])
    df_batch = spark.createDataFrame(despesas_lista, schema=schema)
    df_batch_dedup = df_batch.dropDuplicates(["codDocumento", "_deputado_id"])
    despesas_limpas = [row.asDict() for row in df_batch_dedup.collect()]
    
    # Define modo
    if first_checkpoint and not tabela_existe:
        mode = "overwrite"
    else:
        mode = "append"
    
    save_to_bronze(despesas_limpas, "despesas", "/deputados/{id}/despesas", mode=mode)
    
    duplicados_removidos = len(despesas_lista) - len(despesas_limpas)
    total_gravado += len(despesas_limpas)
    print(f"      ✅ Gravados: {len(despesas_limpas):,} | Duplicados removidos: {duplicados_removidos}")
    
    despesas_lista = []

print()
print("="*70)
print(f"   ✅ CONCLUÍDO: {total_gravado:,} despesas gravadas")
if total_filtrados > 0:
    print(f"   ⚠️  {total_filtrados} registros nulos/inválidos ignorados")
if erros_rede > 0:
    print(f"   ⚠️  {erros_rede} erros de rede")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Gravacao dos Dados
# MAGIC %md
# MAGIC # Otimização da Tabela Bronze

# COMMAND ----------

# DBTITLE 1,Otimiza Tabela Despesas
# ============================================================
# OTIMIZAÇÃO DA TABELA BRONZE
# ============================================================
# Executa OPTIMIZE e ANALYZE para melhorar performance.
# ============================================================

if total_gravado > 0:
    print("Otimizando tabela despesas...")
    print()
    
    try:
        # OPTIMIZE: Compacta arquivos Delta
        print("   Executando OPTIMIZE...")
        spark.sql(f"OPTIMIZE {CATALOG}.{BRONZE_SCHEMA}.despesas")
        print("   ✅ OPTIMIZE concluído")
        
        # ANALYZE: Atualiza estatísticas
        print("   Executando ANALYZE TABLE...")
        spark.sql(f"ANALYZE TABLE {CATALOG}.{BRONZE_SCHEMA}.despesas COMPUTE STATISTICS")
        print("   ✅ ANALYZE TABLE concluído")
        
        print()
        print("   🎯 Tabela otimizada com sucesso!")
        
    except Exception as e:
        print(f"   ⚠️  Aviso: Otimização falhou - {str(e)[:100]}")
else:
    print("⏭️  Pulando otimização (nenhum dado gravado)")

# COMMAND ----------

# DBTITLE 1,Validacao de Qualidade
# MAGIC %md
# MAGIC # Validação de Qualidade
# MAGIC
# MAGIC Verificação de completude, duplicatas e integridade dos dados gravados.

# COMMAND ----------

# DBTITLE 1,Sobre Tratamento de Duplicados
# MAGIC %md
# MAGIC ## 🔒 Tratamento de Duplicados
# MAGIC
# MAGIC Este notebook implementa **prevenção de duplicados em 2 camadas**:
# MAGIC
# MAGIC ### 1️⃣ **Deduplicação nos Checkpoints** (Célula 15)
# MAGIC * **Chave composta**: `(codDocumento, _deputado_id)`
# MAGIC   * `codDocumento` = ID único da despesa
# MAGIC   * `_deputado_id` = ID do deputado
# MAGIC * **Método**: `.dropDuplicates(["codDocumento", "_deputado_id"])` antes de cada gravação
# MAGIC * **Frequência**: Aplicado a cada checkpoint (10.000 despesas) e na gravação final
# MAGIC * **Resultado esperado**: Zero duplicatas
# MAGIC
# MAGIC ### 2️⃣ **Validação Pós-Gravação** (Célula 18)
# MAGIC * **Método**: Conta registros totais vs combinações únicas
# MAGIC * **Alerta**: Exibe aviso se duplicatas forem encontradas
# MAGIC * **Benefício**: Detecta duplicatas de execuções anteriores
# MAGIC
# MAGIC ### 📊 **Por que esta chave?**
# MAGIC * **codDocumento** identifica unicamente cada despesa
# MAGIC * **_deputado_id** garante que despesas de deputados diferentes não sejam confundidas
# MAGIC * Mesmo se um deputado tiver duas despesas com valores/datas idênticas, o `codDocumento` as diferencia
# MAGIC
# MAGIC ### ⚠️ **Importante**
# MAGIC * O tratamento só funciona se `codDocumento` não for nulo
# MAGIC * Registros com `codDocumento` nulo são **filtrados** antes da gravação (célula 15)

# COMMAND ----------

# DBTITLE 1,Diagnostico de Qualidade Despesas
# ============================================================
# DIAGNÓSTICO DE QUALIDADE - DESPESAS
# ============================================================
# Valida completude, duplicatas e integridade dos dados.
# ============================================================

if total_gravado > 0:
    print("="*80)
    print("DIAGNÓSTICO DE QUALIDADE - DESPESAS")
    print("="*80)
    print()
    
    try:
        df_despesas = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.despesas")
        
        # Métricas básicas
        total_registros = df_despesas.count()
        print(f"1️⃣ VOLUME DE DADOS")
        print("-" * 80)
        print(f"   Total de registros: {total_registros:,}")
        print(f"   Registros gravados nesta execução: {total_gravado:,}")
        print()
        
        # Deputados
        deputados_com_despesas = df_despesas.select("_deputado_id").distinct().count()
        print(f"2️⃣ COBERTURA")
        print("-" * 80)
        print(f"   Deputados com despesas: {deputados_com_despesas}/{len(dep_ids)}")
        cobertura_pct = (deputados_com_despesas / len(dep_ids)) * 100
        print(f"   Cobertura: {cobertura_pct:.1f}%")
        print()
        
        # Duplicatas
        print(f"3️⃣ DUPLICATAS")
        print("-" * 80)
        total = df_despesas.count()
        unicos = df_despesas.select("codDocumento", "_deputado_id").distinct().count()
        duplicatas = total - unicos
        
        if duplicatas > 0:
            print(f"   ❌ PROBLEMA: {duplicatas:,} duplicatas encontradas!")
            print(f"   Registros únicos: {unicos:,}")
            print(f"   Taxa de duplicação: {(duplicatas/total)*100:.2f}%")
        else:
            print(f"   ✅ Nenhuma duplicata")
            print(f"   Combinações (codDocumento, _deputado_id) únicas: {unicos:,}")
        print()
        
        # Campos nulos
        print(f"4️⃣ CAMPOS CRÍTICOS")
        print("-" * 80)
        nulos_codDocumento = df_despesas.filter("codDocumento IS NULL").count()
        nulos_tipoDespesa = df_despesas.filter("tipoDespesa IS NULL").count()
        nulos_valorLiquido = df_despesas.filter("valorLiquido IS NULL").count()
        
        problemas_campo = False
        if nulos_codDocumento > 0:
            print(f"   ❌ {nulos_codDocumento:,} registros com codDocumento nulo")
            problemas_campo = True
        if nulos_tipoDespesa > 0:
            print(f"   ❌ {nulos_tipoDespesa:,} registros com tipoDespesa nulo")
            problemas_campo = True
        if nulos_valorLiquido > 0:
            print(f"   ⚠️  {nulos_valorLiquido:,} registros com valorLiquido nulo")
            problemas_campo = True
        
        if not problemas_campo:
            print(f"   ✅ Nenhum nulo em campos críticos")
        print()
        
        # Período
        print(f"5️⃣ PERÍODO")
        print("-" * 80)
        periodo = spark.sql(f"""
            SELECT 
                MIN(CAST(ano AS INT)) as ano_min,
                MAX(CAST(ano AS INT)) as ano_max,
                COUNT(DISTINCT CONCAT(ano, '-', LPAD(mes, 2, '0'))) as periodos
            FROM {CATALOG}.{BRONZE_SCHEMA}.despesas
            WHERE ano IS NOT NULL AND mes IS NOT NULL
        """).collect()[0]
        
        print(f"   Período: {periodo['ano_min']} até {periodo['ano_max']}")
        print(f"   Períodos distintos: {periodo['periodos']}")
        print()
        
        # Top tipos de despesa
        print(f"6️⃣ TOP 5 TIPOS DE DESPESA")
        print("-" * 80)
        top_tipos = spark.sql(f"""
            SELECT tipoDespesa, COUNT(*) as qtd
            FROM {CATALOG}.{BRONZE_SCHEMA}.despesas
            WHERE tipoDespesa IS NOT NULL
            GROUP BY tipoDespesa
            ORDER BY qtd DESC
            LIMIT 5
        """).collect()
        
        for i, row in enumerate(top_tipos, 1):
            print(f"   {i}. {row['tipoDespesa'][:50]:50} - {row['qtd']:,} registros")
        
        print()
        print("="*80)
        print("✅ DIAGNÓSTICO CONCLUÍDO")
        print("="*80)
        
    except Exception as e:
        print(f"❌ Erro ao executar diagnóstico: {str(e)[:200]}")
else:
    print("⏭️  Pulando diagnóstico (nenhum dado gravado)")

# COMMAND ----------

# DBTITLE 1,Atualizacao do Watermark
# MAGIC %md
# MAGIC ## Atualizacao do Controle Incremental

# COMMAND ----------

# DBTITLE 1,Sobre a Atualizacao
# MAGIC %md
# MAGIC Na celula abaixo e registrado o ultimo ano/mes processado para controle incremental.

# COMMAND ----------

# DBTITLE 1,Atualiza Watermark
# ============================================================
# ATUALIZA CONTROLE INCREMENTAL
# ============================================================
# Registra o ultimo ano/mes processado.
# ============================================================

# Atualiza watermark com base no último período processado
if total_gravado > 0:
    # Pega o último ano e mês do período configurado
    last_ano = max(anos)
    last_mes = max(meses)
    
    # Atualiza watermark no formato YYYY-MM-01
    set_watermark("despesas", f"{last_ano}-{last_mes:02d}-01")
    
    # Informa usuario
    print(f"   Watermark atualizado: {last_ano}-{last_mes:02d}")
else:
    print("   Nenhum dado processado - watermark não atualizado")

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
