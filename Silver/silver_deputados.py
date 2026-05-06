# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Silver - Dimensao Deputados (SCD Type 1)
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook transforma os dados brutos de deputados (Bronze) em uma dimensao limpa e tipada.
# MAGIC Aplica SCD Type 1 (Slowly Changing Dimension): quando um deputado muda de partido ou situacao,
# MAGIC o registro e simplesmente atualizado (sem manter historico de versoes).
# MAGIC As transformacoes incluem: selecao de campos, conversao de tipos, padronizacao (upper/trim)
# MAGIC e geracao de surrogate key via MD5.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `dt0025_dev.ft_bronze.deputados_detalhes` | Dados brutos com flatten de ultimoStatus e gabinete |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_silver.dim_deputados` | Dimensao limpa com surrogate key e campos padronizados |
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
log_notebook_start("silver_deputados")

# COMMAND ----------

# DBTITLE 1,Leitura dos Dados
# MAGIC %md
# MAGIC # Leitura da Camada Bronze

# COMMAND ----------

# DBTITLE 1,Sobre a Leitura
# MAGIC %md
# MAGIC Na celula abaixo e lida a tabela de deputados detalhados da camada Bronze.
# MAGIC Esta tabela ja possui o flatten (achatamento) das estruturas aninhadas
# MAGIC da API (ultimoStatus, gabinete) em colunas individuais.

# COMMAND ----------

# DBTITLE 1,Le Bronze Deputados
# ============================================================
# LEITURA DA CAMADA BRONZE
# ============================================================
# Le a tabela bronze de deputados detalhados com todas
# as informacoes pessoais, partidarias e de gabinete.
# ============================================================

# Informa o usuario
print("Lendo bronze.deputados_detalhes...")

# Le a tabela da camada Bronze
df_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.deputados_detalhes")

# Exibe quantidade de registros
print(f"   Registros: {df_bronze.count()}")

# COMMAND ----------

# DBTITLE 1,Transformacao dos Dados
# MAGIC %md
# MAGIC # Transformacao dos Dados

# COMMAND ----------

# DBTITLE 1,Sobre a Transformacao
# MAGIC %md
# MAGIC Na celula abaixo sao aplicadas as seguintes transformacoes:
# MAGIC 1. **Selecao de campos**: apenas os campos relevantes para a dimensao
# MAGIC 2. **Conversao de tipos**: ID para LONG, datas para DATE
# MAGIC 3. **Padronizacao**: UF e partido em maiusculas sem espacos
# MAGIC 4. **Surrogate key**: hash MD5 do ID para uso como chave da dimensao
# MAGIC 5. **Deduplicacao**: remove registros duplicados pelo ID

# COMMAND ----------

# DBTITLE 1,Transforma Silver
# ============================================================
# TRANSFORMACAO E LIMPEZA
# ============================================================
# Aplica transformacoes para criar a dimensao de deputados
# com campos limpos, tipados e padronizados.
# ============================================================

# Informa o usuario
print("Transformando dados...")

# Aplica todas as transformacoes em cadeia (pipeline PySpark)
df_silver = (df_bronze
    # Seleciona e renomeia os campos relevantes
    .select(
        # ID do deputado convertido para LONG (numerico)
        col("id").cast("long").alias("id_deputado"),
        # Nome civil completo
        col("nomeCivil").alias("nome_civil"),
        # Nome parlamentar (como e conhecido na Camara)
        col("status_nome").alias("nome_parlamentar"),
        # CPF do deputado
        col("cpf"),
        # Sexo (M/F)
        col("sexo"),
        # Data de nascimento
        col("dataNascimento").alias("data_nascimento"),
        # Municipio onde nasceu
        col("municipioNascimento").alias("municipio_nascimento"),
        # UF onde nasceu
        col("ufNascimento").alias("uf_nascimento"),
        # Nivel de escolaridade
        col("escolaridade"),
        # Sigla do partido atual
        col("status_siglaPartido").alias("sigla_partido"),
        # UF que representa
        col("status_siglaUf").alias("sigla_uf"),
        # Situacao atual (Exercicio, Suplencia, etc)
        col("status_situacao").alias("situacao"),
        # Condicao eleitoral
        col("status_condicaoEleitoral").alias("condicao_eleitoral"),
        # Nome de urna
        col("status_nomeEleitoral").alias("nome_eleitoral"),
        # Dados do gabinete
        col("gabinete_nome").alias("gabinete_nome"),
        col("gabinete_predio").alias("gabinete_predio"),
        col("gabinete_sala").alias("gabinete_sala"),
        col("gabinete_andar").alias("gabinete_andar"),
        col("gabinete_telefone").alias("gabinete_telefone"),
        col("gabinete_email").alias("gabinete_email")
    )
    # Remove registros sem ID (dados invalidos)
    .filter(col("id_deputado").isNotNull())
    # Padroniza partido em maiusculas sem espacos
    .withColumn("sigla_partido", upper(trim(col("sigla_partido"))))
    # Padroniza UF em maiusculas sem espacos
    .withColumn("sigla_uf", upper(trim(col("sigla_uf"))))
    # Gera surrogate key via hash MD5 do ID
    .withColumn("sk_deputado", md5(concat_ws("||", col("id_deputado").cast("string"))))
    # Remove duplicatas pelo ID do deputado
    .dropDuplicates(["id_deputado"])
)

# Exibe quantidade apos transformacao
print(f"   Registros apos transformacao: {df_silver.count()}")

# COMMAND ----------

# DBTITLE 1,Validacao de Qualidade
# MAGIC %md
# MAGIC # Validacao de Qualidade

# COMMAND ----------

# DBTITLE 1,Sobre a Validacao
# MAGIC %md
# MAGIC Na celula abaixo e verificada a qualidade dos dados transformados antes de gravar.
# MAGIC Campos obrigatorios: id_deputado, nome_parlamentar, sigla_partido, sigla_uf.

# COMMAND ----------

# DBTITLE 1,Verifica Qualidade
# ============================================================
# VALIDACAO DE QUALIDADE
# ============================================================
# Verifica campos obrigatorios e integridade dos dados
# antes de gravar na camada Silver.
# ============================================================

# Executa validacao de qualidade
check_quality(df_silver, "dim_deputados",
    key_columns=["id_deputado"],
    critical_columns=["id_deputado", "nome_parlamentar", "sigla_partido", "sigla_uf"])

# COMMAND ----------

# DBTITLE 1,Gravacao na Silver
# MAGIC %md
# MAGIC # Gravacao na Camada Silver (MERGE)

# COMMAND ----------

# DBTITLE 1,Sobre o Merge
# MAGIC %md
# MAGIC Na celula abaixo e executado o MERGE (upsert) na tabela Silver.
# MAGIC O MERGE compara cada registro pelo `id_deputado`:
# MAGIC - Se ja existe: atualiza todos os campos (SCD Type 1)
# MAGIC - Se e novo: insere o registro

# COMMAND ----------

# DBTITLE 1,Executa Merge Silver
# ============================================================
# MERGE INCREMENTAL NA SILVER
# ============================================================
# Realiza upsert por id_deputado. Registros existentes sao
# atualizados se houver mudanca. Novos sao inseridos.
# ============================================================

# Executa o MERGE usando a chave primaria id_deputado
merge_to_silver(df_silver, "dim_deputados", key_columns=["id_deputado"])

# Registra status para o resumo final
status_list.append({"tabela": "ft_silver.dim_deputados", "registros": df_silver.count()})

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

