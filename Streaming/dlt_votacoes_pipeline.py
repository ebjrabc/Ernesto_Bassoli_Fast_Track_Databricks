# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Qualidade de Dados - Votacoes (Bronze > Prata > Ouro)
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook implementa validacao de qualidade de dados em 3 camadas (Bronze, Prata, Ouro)
# MAGIC para as votacoes da Camara dos Deputados. Utiliza PySpark puro (sem DLT) para funcionar
# MAGIC no Databricks Free Edition. As validacoes incluem 10 regras de qualidade equivalentes
# MAGIC as expectations do DLT: nulos, duplicatas, ranges de data e classificacao de urgencia.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|-----------|
# MAGIC | `dt0025_dev.ft_bronze.votacoes` | Votacoes brutas ingeridas |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|-----------|
# MAGIC | `dt0025_dev.ft_silver.votacoes_quality` | Votacoes limpas com qualidade validada |
# MAGIC | `dt0025_dev.ft_gold.votacoes_metricas` | Metricas diarias por orgao |
# MAGIC | `dt0025_dev.ft_gold.votacoes_alertas` | Alertas de votacoes urgentes |
# MAGIC
# MAGIC ## Regras de Qualidade (10 Expectations)
# MAGIC | Regra | Acao | Descricao |
# MAGIC |-------|------|-----------|
# MAGIC | id_valido | DROP | Remove registros com id nulo |
# MAGIC | data_valida | WARN | Avisa se data e nula |
# MAGIC | id_unico | DROP | Remove duplicatas por id |
# MAGIC | orgao_preenchido | WARN | Avisa se orgao esta vazio |
# MAGIC | data_legislatura_57 | WARN | Avisa se data fora do range |
# MAGIC | id_positivo | FAIL | Falha se id <= 0 |
# MAGIC | ao_menos_uma_votacao | WARN | Avisa se grupo tem 0 votacoes |
# MAGIC | orgao_valido | WARN | Avisa se orgao nulo na Gold |
# MAGIC | urgencia_alta | DROP | Filtra apenas urgencia CRITICA/ALTA |
# MAGIC | proposicao_presente | WARN | Avisa se proposicao nula |
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
# MAGIC o conteudo do arquivo FUNCOES_GENERICAS.py, disponibilizando todas as funcoes para uso.

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
log_notebook_start("dlt_votacoes_pipeline")

# COMMAND ----------

# DBTITLE 1,Camada Bronze
# MAGIC %md
# MAGIC # Camada Bronze - Leitura e Validacao Inicial

# COMMAND ----------

# DBTITLE 1,Sobre a Camada Bronze
# MAGIC %md
# MAGIC Na celula abaixo sao lidos os dados brutos de votacoes e aplicadas as primeiras
# MAGIC regras de qualidade: remocao de registros com ID nulo e alerta para datas nulas.

# COMMAND ----------

# DBTITLE 1,Bronze - Votacoes Raw
# ============================================================
# CAMADA BRONZE - VOTACOES BRUTAS COM QUALIDADE
# ============================================================
# Le dados brutos e aplica 2 regras:
# - id_valido: REMOVE registros com id nulo (equivale a expect_or_drop)
# - data_valida: AVISA se data e nula (equivale a expect)
# ============================================================

# Informa o usuario
print("Camada Bronze: lendo votacoes brutas...")

# Le a tabela de votacoes brutas
df_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.votacoes")

# Conta total antes da validacao
total_bruto = df_bronze.count()
print(f"   Registros brutos: {total_bruto}")

# Regra 1: id_valido (DROP - remove nulos)
df_bronze = df_bronze.filter(col("id").isNotNull())
total_pos_filtro = df_bronze.count()
removidos_id = total_bruto - total_pos_filtro
if removidos_id > 0:
    log_warn("QUALITY_DROP", f"id_valido: {removidos_id} registros removidos (id nulo)")
    print(f"   [DROP] id_valido: {removidos_id} removidos")

# Regra 2: data_valida (WARN - apenas alerta)
nulos_data = df_bronze.filter(
    col("data").isNull() & col("dataHoraRegistro").isNull()
).count()
if nulos_data > 0:
    log_warn("QUALITY_WARN", f"data_valida: {nulos_data} registros com data nula")
    print(f"   [WARN] data_valida: {nulos_data} com data nula")

# Adiciona timestamp de processamento
df_bronze = df_bronze.withColumn("_quality_checked_at", current_timestamp())

print(f"   Bronze validado: {df_bronze.count()} registros")

# COMMAND ----------

# DBTITLE 1,Camada Prata
# MAGIC %md
# MAGIC # Camada Prata - Transformacao e Validacao

# COMMAND ----------

# DBTITLE 1,Sobre a Camada Prata
# MAGIC %md
# MAGIC Na celula abaixo os dados sao limpos, tipados e deduplicados. Sao aplicadas 4 regras:
# MAGIC remocao de duplicatas, validacao de orgao, range de data e positividade do ID.

# COMMAND ----------

# DBTITLE 1,Prata - Votacoes Limpas
# ============================================================
# CAMADA PRATA - TRANSFORMACAO COM QUALIDADE
# ============================================================
# Aplica 4 regras:
# - id_unico: REMOVE duplicatas (expect_or_drop)
# - orgao_preenchido: AVISA se vazio (expect)
# - data_legislatura_57: AVISA se fora do range (expect)
# - id_positivo: FALHA se id <= 0 (expect_or_fail)
# ============================================================

# Informa o usuario
print("Camada Prata: transformando e validando...")

# Transforma dados: tipagem, renomeacao, padronizacao
df_prata = (df_bronze
    .select(
        col("id").cast("long").alias("id_votacao"),
        to_date(col("data")).alias("data_votacao"),
        to_timestamp(col("dataHoraRegistro")).alias("data_hora_registro"),
        col("siglaOrgao").alias("sigla_orgao"),
        col("uriEvento").alias("uri_evento"),
        col("proposicaoObjeto").alias("proposicao_objeto")
    )
    .withColumn("sigla_orgao", upper(trim(col("sigla_orgao"))))
    .withColumn("sk_votacao", md5(col("id_votacao").cast("string")))
)

# Regra 3: id_unico (DROP duplicatas)
antes_dedup = df_prata.count()
df_prata = df_prata.dropDuplicates(["id_votacao"])
duplicatas = antes_dedup - df_prata.count()
if duplicatas > 0:
    log_warn("QUALITY_DROP", f"id_unico: {duplicatas} duplicatas removidas")
    print(f"   [DROP] id_unico: {duplicatas} duplicatas removidas")

# Regra 4: orgao_preenchido (WARN)
orgao_vazio = df_prata.filter(
    col("sigla_orgao").isNull() | (col("sigla_orgao") == "")
).count()
if orgao_vazio > 0:
    log_warn("QUALITY_WARN", f"orgao_preenchido: {orgao_vazio} sem orgao")
    print(f"   [WARN] orgao_preenchido: {orgao_vazio} sem orgao")

# Regra 5: data_legislatura_57 (WARN)
fora_range = df_prata.filter(
    col("data_votacao") < "2023-02-01"
).count()
if fora_range > 0:
    log_warn("QUALITY_WARN", f"data_legislatura_57: {fora_range} fora do range")
    print(f"   [WARN] data_legislatura_57: {fora_range} anteriores a 2023-02-01")

# Regra 6: id_positivo (FAIL - interrompe se violado)
ids_negativos = df_prata.filter(col("id_votacao") <= 0).count()
if ids_negativos > 0:
    log_critical("QUALITY_FAIL", f"id_positivo: {ids_negativos} IDs <= 0 - ABORTANDO")
    raise ValueError(f"QUALITY FAIL: {ids_negativos} registros com id_votacao <= 0")

print(f"   Prata validado: {df_prata.count()} registros")

# Grava na Silver
merge_to_silver(df_prata, "votacoes_quality", key_columns=["id_votacao"])
status_processamento("ft_silver.votacoes_quality", df_prata.count())

# COMMAND ----------

# DBTITLE 1,Camada Ouro
# MAGIC %md
# MAGIC # Camada Ouro - Metricas e Alertas

# COMMAND ----------

# DBTITLE 1,Sobre as Metricas
# MAGIC %md
# MAGIC Na celula abaixo sao calculadas metricas diarias de votacoes por orgao e gerados
# MAGIC alertas para votacoes de urgencia critica ou alta.

# COMMAND ----------

# DBTITLE 1,Ouro - Metricas e Alertas
# ============================================================
# CAMADA OURO - METRICAS DIARIAS E ALERTAS
# ============================================================
# Aplica 4 regras:
# - ao_menos_uma_votacao: AVISA se grupo vazio (expect)
# - orgao_valido: AVISA se orgao nulo (expect)
# - urgencia_alta: FILTRA apenas CRITICA/ALTA (expect_or_drop)
# - proposicao_presente: AVISA se proposicao nula (expect)
# ============================================================

# Informa o usuario
print("Camada Ouro: calculando metricas...")

# Metricas diarias por orgao
df_metricas = (df_prata
    .groupBy("data_votacao", "sigla_orgao")
    .agg(
        count("*").alias("total_votacoes"),
        count(when(col("proposicao_objeto").isNotNull(), True)).alias("com_proposicao")
    )
    .withColumn("_processed_at", current_timestamp())
)

# Regra 7: ao_menos_uma_votacao (WARN)
grupos_vazios = df_metricas.filter(col("total_votacoes") == 0).count()
if grupos_vazios > 0:
    log_warn("QUALITY_WARN", f"ao_menos_uma_votacao: {grupos_vazios} grupos vazios")

# Regra 8: orgao_valido (WARN)
orgao_nulo_gold = df_metricas.filter(col("sigla_orgao").isNull()).count()
if orgao_nulo_gold > 0:
    log_warn("QUALITY_WARN", f"orgao_valido: {orgao_nulo_gold} sem orgao na Gold")

# Grava metricas
save_to_gold(df_metricas, "votacoes_metricas")
status_processamento("ft_gold.votacoes_metricas", df_metricas.count())

# Alertas de urgencia (filtra apenas CRITICA e ALTA)
df_alertas = (df_prata
    .filter(col("proposicao_objeto").isNotNull())
    .withColumn("urgencia_classificada",
        when(col("sigla_orgao") == "PLEN", "CRITICA")
        .when(col("sigla_orgao").isNotNull(), "ALTA")
        .otherwise("NORMAL")
    )
    .filter(col("urgencia_classificada").isin("CRITICA", "ALTA"))
    .withColumn("_alert_generated_at", current_timestamp())
)

# Regra 9 e 10 aplicadas via filter acima
print(f"   Alertas gerados: {df_alertas.count()}")

# Grava alertas
save_to_gold(df_alertas, "votacoes_alertas")
status_processamento("ft_gold.votacoes_alertas", df_alertas.count())

print(f"   Metricas: {df_metricas.count()} | Alertas: {df_alertas.count()}")

# COMMAND ----------

# DBTITLE 1,Finalizacao
# MAGIC %md
# MAGIC # Finalizacao do Notebook

# COMMAND ----------

# DBTITLE 1,Sobre a Finalizacao
# MAGIC %md
# MAGIC Na celula abaixo e encerrado o processamento com resumo das validacoes de qualidade.

# COMMAND ----------

# DBTITLE 1,Finaliza Notebook
# ============================================================
# FINALIZACAO E RESUMO
# ============================================================
finalizar_notebook()

