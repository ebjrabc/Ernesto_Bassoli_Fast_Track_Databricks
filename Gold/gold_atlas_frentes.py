# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Gold - Atlas das Frentes Parlamentares
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook gera analises avancadas sobre frentes parlamentares:
# MAGIC indice de Herfindahl (diversidade partidaria), deputados multi-frentes,
# MAGIC matriz de sobreposicao entre frentes e evolucao por legislatura.
# MAGIC O indice de Herfindahl mede a concentracao: quanto menor, mais diversa a frente.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `dt0025_dev.ft_silver.dim_deputados` | Dimensao deputados com partido |
# MAGIC | `dt0025_dev.ft_bronze.frentes_membros` | Membros de cada frente |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `dt0025_dev.ft_gold.atlas_frentes_herfindahl` | Indice de diversidade por frente |
# MAGIC | `dt0025_dev.ft_gold.atlas_frentes_multi` | Deputados em multiplas frentes |
# MAGIC | `dt0025_dev.ft_gold.atlas_frentes_overlap` | Matriz de sobreposicao |
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
log_notebook_start("gold_atlas_frentes")

# COMMAND ----------

# DBTITLE 1,Sobre: Tabela Gold Frentes
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Tabela Gold Frentes**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Tabela Gold Frentes
# ============================================================
# TABELA GOLD: FRENTES COM MEMBROS
# ============================================================
# Consolida frentes com seus membros incluindo partido,
# UF e legislatura. Grain: 1 registro por (frente, membro).
# ============================================================

print("📖 Construindo atlas_frentes...")

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_BRONZE}.frentes"
df_frentes = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.frentes")
# Carrega dados da tabela f"{CATALOG}.{SCHEMA_BRONZE}.frentes_membros"
df_membros = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.frentes_membros")

# Inicia cadeia de transformacoes PySpark
df_atlas = (df_membros
    # Seleciona as colunas desejadas para o resultado
    .select(
        # Referencia a coluna '_frente_id'
        col("_frente_id").cast("long").alias("id_frente"),
        # Referencia a coluna '_frente_titulo'
        col("_frente_titulo").alias("titulo_frente"),
        # Referencia a coluna 'id'
        col("id").cast("long").alias("id_deputado"),
        # Referencia a coluna 'nome'
        col("nome").alias("nome_deputado"),
        # Referencia a coluna 'siglaPartido'
        col("siglaPartido").alias("sigla_partido"),
        # Referencia a coluna 'siglaUf'
        col("siglaUf").alias("sigla_uf"),
        # Referencia a coluna 'titulo'
        col("titulo").alias("titulo_na_frente"),
        # Referencia a coluna 'idLegislatura'
        col("idLegislatura").cast("int").alias("id_legislatura")
    # Fecha bloco de parametros
    )
    # Filtra registros conforme condicao especificada
    .filter(col("id_frente").isNotNull())
    # Filtra registros conforme condicao especificada
    .filter(col("id_deputado").isNotNull())
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_atlas, "atlas_frentes")
# Registra status para o resumo final
status_processamento("ft_gold.atlas_frentes", df_atlas.count())

# COMMAND ----------

# DBTITLE 1,Sobre: Índice Herfindahl
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Índice Herfindahl**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Índice Herfindahl
# ============================================================
# DIVERSIDADE PARTIDÁRIA — ÍNDICE DE HERFINDAHL
# ============================================================
# Calcula o índice de Herfindahl-Hirschman (HHI) para cada
# frente. HHI = soma dos quadrados das participações de
# cada partido. Menor HHI = maior diversidade.
# Fórmula: HHI = Σ(ni/N)² onde ni = membros do partido i
# Normalizado: 1/HHI dá o "número efetivo de partidos".
# ============================================================

print("📊 Calculando diversidade partidária (Herfindahl)...")

# Conta membros por partido em cada frente
w_frente = Window.partitionBy("id_frente")

# Inicia cadeia de transformacoes PySpark
df_partido_count = (df_atlas
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_frente", "titulo_frente", "sigla_partido")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("n_membros_partido"))
# Fecha bloco de parametros
)

# Inicia cadeia de transformacoes PySpark
df_total_frente = (df_atlas
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_frente")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("n_membros_total"))
# Fecha bloco de parametros
)

# Calcula HHI
df_hhi = (df_partido_count
    # Faz join (cruzamento) com outra tabela
    .join(df_total_frente, "id_frente")
    # Adiciona ou modifica a coluna 'share'
    .withColumn("share", col("n_membros_partido") / col("n_membros_total"))
    # Adiciona ou modifica a coluna 'share_squared'
    .withColumn("share_squared", col("share") * col("share"))
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_frente", "titulo_frente", "n_membros_total")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(
        # Soma valores
        spark_sum("share_squared").alias("hhi"),
        # Conta registros
        count("sigla_partido").alias("n_partidos")
    # Fecha bloco de parametros
    )
    # Adiciona ou modifica a coluna 'diversidade_efetiva'
    .withColumn("diversidade_efetiva", lit(1.0) / col("hhi"))
    # Ordena resultados
    .orderBy(col("hhi").asc())
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_hhi, "frentes_diversidade")
# Registra status para o resumo final
status_processamento("ft_gold.frentes_diversidade", df_hhi.count())

# Exibe mensagem informativa para o usuario
print("\n   TOP 10 frentes MAIS diversas (menor HHI):")
# Exibe amostra dos resultados no console
df_hhi.select("titulo_frente", "n_partidos", "hhi", "diversidade_efetiva").show(10, truncate=50)

# COMMAND ----------

# DBTITLE 1,Sobre: Deputados Multi-Frentes
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Deputados Multi-Frentes**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Deputados Multi-Frentes
# ============================================================
# DEPUTADOS QUE PARTICIPAM DE MAIS FRENTES
# ============================================================
# Identifica deputados com participação em múltiplas
# frentes parlamentares. Revela perfil de atuação e
# temas de interesse de cada parlamentar.
# ============================================================

print("📊 Identificando deputados multi-frentes...")

# Inicia cadeia de transformacoes PySpark
df_multi = (df_atlas
    # Agrupa registros pelas colunas indicadas
    .groupBy("id_deputado", "nome_deputado", "sigla_partido", "sigla_uf")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(
        # Conta registros
        count("id_frente").alias("n_frentes"),
        # Renomeia coluna para 'lista_frentes'
        collect_list("titulo_frente").alias("lista_frentes")
    # Fecha bloco de parametros
    )
    # Ordena resultados
    .orderBy(col("n_frentes").desc())
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_multi, "deputados_multi_frentes")
# Registra status para o resumo final
status_processamento("ft_gold.deputados_multi_frentes", df_multi.count())

# Exibe mensagem informativa para o usuario
print("\n   TOP 10 deputados com mais frentes:")
# Exibe amostra dos resultados no console
df_multi.select("nome_deputado", "sigla_partido", "sigla_uf", "n_frentes").show(10)

# COMMAND ----------

# DBTITLE 1,Sobre: Sobreposição Entre Frentes
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Sobreposição Entre Frentes**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Sobreposição Entre Frentes
# ============================================================
# SOBREPOSIÇÃO DE MEMBROS ENTRE FRENTES
# ============================================================
# Identifica deputados que participam de frentes com
# temáticas opostas (ex: agronegócio vs meio ambiente).
# Usa self-join para encontrar pares de frentes com
# alto índice de membros compartilhados.
# ============================================================

print("📊 Analisando sobreposição entre frentes...")

# Self-join para encontrar pares de frentes
df_pairs = (df_atlas.alias("a")
    # Faz join (cruzamento) com outra tabela
    .join(df_atlas.alias("b"),
        # Atribui valor a variavel '(col("a.id_deputado")'
        (col("a.id_deputado") == col("b.id_deputado")) &
        # Executa operacao de processamento
        (col("a.id_frente") < col("b.id_frente"))
    # Fecha bloco de parametros
    )
    # Agrupa registros pelas colunas indicadas
    .groupBy(
        # Referencia a coluna 'a.id_frente'
        col("a.id_frente").alias("frente_a_id"),
        # Referencia a coluna 'a.titulo_frente'
        col("a.titulo_frente").alias("frente_a"),
        # Referencia a coluna 'b.id_frente'
        col("b.id_frente").alias("frente_b_id"),
        # Referencia a coluna 'b.titulo_frente'
        col("b.titulo_frente").alias("frente_b")
    # Fecha bloco de parametros
    )
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("membros_compartilhados"))
    # Ordena resultados
    .orderBy(col("membros_compartilhados").desc())
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_pairs, "frentes_sobreposicao")
# Registra status para o resumo final
status_processamento("ft_gold.frentes_sobreposicao", df_pairs.count())

# Exibe mensagem informativa para o usuario
print("\n   TOP 10 pares de frentes com mais membros em comum:")
# Exibe amostra dos resultados no console
df_pairs.select("frente_a", "frente_b", "membros_compartilhados").show(10, truncate=40)

# COMMAND ----------

# DBTITLE 1,Sobre: Evolução Frentes Legislaturas
# MAGIC %md
# MAGIC Na celula abaixo e realizada a operacao de **Evolução Frentes Legislaturas**.
# MAGIC Esta etapa faz parte do processamento analitico dos dados.

# COMMAND ----------

# DBTITLE 1,Evolução Frentes Legislaturas
# ============================================================
# EVOLUÇÃO DO NÚMERO DE FRENTES POR TEMA ENTRE LEGISLATURAS
# ============================================================
# Analisa como o número de frentes parlamentares evoluiu
# ao longo das legislaturas. Agrupa frentes por palavras-chave
# no título para identificar temas (Agropecuária, Saúde,
# Educação, Segurança, Meio Ambiente, etc.).
# Permite identificar crescimento/declínio de temas.
# ============================================================

print("📊 Analisando evolução de frentes por legislatura...")

# Busca frentes de TODAS as legislaturas (não só a atual)
from pyspark.sql.functions import when, regexp_extract

# Carrega dados da tabela f"{CATALOG}.{SCHEMA_BRONZE}.frentes"
df_todas_frentes = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.frentes")

# Classifica por tema usando palavras-chave no título
df_frentes_tema = (df_todas_frentes
    # Adiciona ou modifica a coluna 'tema'
    .withColumn("tema",
        # Executa operacao de processamento
        when(col("titulo").rlike("(?i)agr[oí]|rural|pecuári|campo"), "Agropecuária")
        # Executa operacao de processamento
        .when(col("titulo").rlike("(?i)saúde|sa[uú]de|médic|hospital"), "Saúde")
        # Executa operacao de processamento
        .when(col("titulo").rlike("(?i)educa[çc]|escola|universidade|ensino"), "Educação")
        # Executa operacao de processamento
        .when(col("titulo").rlike("(?i)seguran[çc]|polí[cç]|defesa"), "Segurança")
        # Executa operacao de processamento
        .when(col("titulo").rlike("(?i)meio ambiente|ambiental|sustent|ecolog"), "Meio Ambiente")
        # Executa operacao de processamento
        .when(col("titulo").rlike("(?i)tecnolog|digital|inova[çc]|cyber"), "Tecnologia")
        # Executa operacao de processamento
        .when(col("titulo").rlike("(?i)mulher|gênero|feminino|matern"), "Direitos da Mulher")
        # Executa operacao de processamento
        .when(col("titulo").rlike("(?i)criança|adolescen|juvenil|infância"), "Infância")
        # Executa operacao de processamento
        .when(col("titulo").rlike("(?i)trabalh|emprego|sindic|CLT"), "Trabalho")
        # Executa operacao de processamento
        .when(col("titulo").rlike("(?i)esport|futebol|olímpic"), "Esporte")
        # Executa operacao de processamento
        .otherwise("Outros")
    # Fecha bloco de parametros
    )
    # Agrupa registros pelas colunas indicadas
    .groupBy("idLegislatura", "tema")
    # Calcula agregacoes (soma, contagem, media) por grupo
    .agg(count("*").alias("n_frentes"))
    # Ordena resultados
    .orderBy("idLegislatura", col("n_frentes").desc())
# Fecha bloco de parametros
)

# Grava resultado na tabela Gold
save_to_gold(df_frentes_tema, "evolucao_frentes_legislatura")
# Registra status para o resumo final
status_processamento("ft_gold.evolucao_frentes_legislatura", df_frentes_tema.count())

# Exibe mensagem informativa para o usuario
print("\n   Evolução por legislatura:")
# Exibe amostra dos resultados no console
df_frentes_tema.show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Finaliza Notebook
# ============================================================
# FINALIZAÇÃO E RESUMO
# ============================================================
# Exibe métricas de tempo e registros processados.
# ============================================================

finalizar_notebook()
