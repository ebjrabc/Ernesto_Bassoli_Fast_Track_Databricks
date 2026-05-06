# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

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

# DBTITLE 1,Ingere Lista Frentes
# ============================================================
# INGESTAO DA LISTA DE FRENTES PARLAMENTARES
# ============================================================
# Busca todas as frentes parlamentares da legislatura 57.
# Cada frente possui id, titulo e legislatura associada.
# Dados usados para mapear bancadas tematicas.
# ============================================================

# Informa o usuario que a ingestao esta iniciando
print("Ingerindo lista de frentes parlamentares...")

# Busca todas as frentes da legislatura atual via API
frentes_lista = fetch_api(
    endpoint="/frentes",
    params={"idLegislatura": 57}
)

# Exibe quantidade de frentes encontradas
print(f"   Frentes encontradas: {len(frentes_lista)}")

# COMMAND ----------

# DBTITLE 1,Sobre os Membros das Frentes
# MAGIC %md
# MAGIC Na celula abaixo, para cada frente parlamentar, sao buscados seus membros
# MAGIC (deputados que aderiram). Cada membro possui nome, partido, UF e titulo na frente.
# MAGIC Esses dados sao essenciais para calcular a diversidade partidaria (indice de Herfindahl)
# MAGIC e a sobreposicao entre frentes.

# COMMAND ----------

# DBTITLE 1,Ingere Membros Frentes
# ============================================================
# INGESTAO DOS MEMBROS DE CADA FRENTE
# ============================================================
# Para cada frente, busca a lista de membros (deputados)
# com partido, UF, titulo na frente e periodo de participacao.
# Essencial para analise de diversidade partidaria (Herfindahl)
# e sobreposicao entre frentes.
# ============================================================

# Informa o usuario que a ingestao de membros esta iniciando
print("Ingerindo membros de cada frente...")

# Lista para acumular todos os membros de todas as frentes
frentes_membros = []

# Total de frentes para calcular progresso percentual
total = len(frentes_lista)

# Loop: percorre cada frente para buscar seus membros
for i, frente in enumerate(frentes_lista):
    # Extrai o ID da frente atual
    frente_id = frente['id']
    
    try:
        # Busca os membros desta frente via API
        dados = fetch_api(f"/frentes/{frente_id}/membros")
        
        # Para cada membro, adiciona campos de referencia
        for d in dados:
            # Adiciona o ID da frente como campo auxiliar
            d['_frente_id'] = frente_id
            # Adiciona o titulo da frente como campo auxiliar
            d['_frente_titulo'] = frente.get('titulo', '')
        
        # Adiciona os membros desta frente a lista geral
        frentes_membros.extend(dados)
        
    # Erro de conexao (rede indisponivel)
    except requests.exceptions.ConnectionError:
        # Informa o usuario e interrompe o loop
        print(f"  ERRO DE CONEXAO na frente {frente_id} - abortando")
        break
        
    # Qualquer outro erro
    except Exception as e:
        # Informa o usuario e continua com a proxima frente
        print(f"  Erro na frente {frente_id}: {str(e)[:60]}")
    
    # A cada 50 frentes, exibe progresso
    if (i + 1) % 50 == 0:
        # Calcula e exibe percentual concluido
        print(f"   Progresso: {i+1}/{total} ({(i+1)*100//total}%)")
        # Pequena pausa para nao sobrecarregar a API
        time.sleep(0.3)

# Exibe total de membros obtidos
print(f"   Total membros: {len(frentes_membros)} registros")

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
# GRAVACAO NA CAMADA BRONZE
# ============================================================
# Grava frentes e membros em tabelas separadas na camada
# Bronze. Mantem dados brutos da API com campos de auditoria.
# ============================================================

# Grava a lista de frentes na tabela bronze
n1 = save_to_bronze(frentes_lista, "frentes", "/frentes")

# Registra status para o resumo final
status_list.append({"tabela": "ft_bronze.frentes", "registros": n1})

# Grava os membros de todas as frentes na tabela bronze
n2 = save_to_bronze(frentes_membros, "frentes_membros", "/frentes/{id}/membros")

# Registra status para o resumo final
status_list.append({"tabela": "ft_bronze.frentes_membros", "registros": n2})

# COMMAND ----------

# DBTITLE 1,Validacao de Qualidade
# MAGIC %md
# MAGIC # Validacao de Qualidade

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

