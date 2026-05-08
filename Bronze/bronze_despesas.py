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

# DBTITLE 1,Ingere Despesas CEAP
# ============================================================
# INGESTAO INCREMENTAL DE DESPESAS POR DEPUTADO
# ============================================================
# Para cada deputado, busca despesas dos ultimos meses.
# Cada despesa contem: tipoDespesa, valorDocumento,
# valorLiquido, fornecedor, CNPJ/CPF, data e documento.
# ============================================================

# Informa o usuario que a ingestao esta iniciando
print("Ingerindo despesas CEAP...")

# Recupera watermark para controle incremental
watermark = get_watermark("despesas")

# Define periodo com base no watermark
if isinstance(watermark, dict) and watermark.get("last_date"):
    # Carga incremental: busca a partir do ultimo mes processado
    anos = [int(watermark["last_date"][:4])]
    meses = list(range(int(watermark["last_date"][5:7]), 13))
    print(f"   Incremental: ano {anos[0]}, meses {meses}")
else:
    # Carga completa: busca todos os anos da legislatura
    anos = [2023, 2024, 2025]
    meses = list(range(1, 13))
    print(f"   Carga completa: anos {anos}")

# Lista para acumular todas as despesas de todos os deputados
despesas_lista = []

# Total de deputados para calculo de progresso
total_deps = len(dep_ids)

# Loop principal: percorre cada deputado
for i, dep_id in enumerate(dep_ids):
    # Para cada ano do periodo
    for ano in anos:
        # Para cada mes do periodo
        for mes in meses:
            try:
                # Busca despesas deste deputado neste ano/mes
                dados = fetch_api(
                    f"/deputados/{dep_id}/despesas",
                    params={"ano": ano, "mes": mes, "itens": 100}
                )
                
                # Para cada despesa, adiciona o ID do deputado
                for d in dados:
                    d['_deputado_id'] = dep_id
                
                # Adiciona as despesas a lista geral
                despesas_lista.extend(dados)
                
            # Erro de conexao (rede indisponivel)
            except requests.exceptions.ConnectionError:
                # Informa o usuario e interrompe este deputado
                print(f"  ERRO DE CONEXAO ao buscar despesas dep {dep_id} - abortando")
                break
                
            # Qualquer outro erro
            except Exception as e:
                # Informa o usuario e continua
                print(f"  Erro despesas dep {dep_id}/{ano}/{mes}: {str(e)[:50]}")
    
    # A cada 50 deputados, exibe progresso
    if (i + 1) % 50 == 0:
        # Calcula e exibe percentual concluido
        print(f"   Progresso: {i+1}/{total_deps} deputados ({(i+1)*100//total_deps}%)")
        # Exibe quantidade acumulada
        print(f"   Despesas acumuladas: {len(despesas_lista)}")
        # Pausa para nao sobrecarregar a API
        time.sleep(0.5)

# Exibe total final de despesas
print(f"   Total despesas: {len(despesas_lista)} registros")

# COMMAND ----------

# DBTITLE 1,Gravacao dos Dados
# MAGIC %md
# MAGIC # Gravacao na Camada Bronze

# COMMAND ----------

# DBTITLE 1,Sobre a Gravacao
# MAGIC %md
# MAGIC Na celula abaixo os dados sao gravados na tabela Bronze.
# MAGIC Se e carga incremental (ja rodou antes), usa `append`. Se e primeira vez, usa `overwrite`.

# COMMAND ----------

# DBTITLE 1,Grava Bronze Despesas
# ============================================================
# GRAVACAO NA CAMADA BRONZE
# ============================================================
# Grava todas as despesas na tabela bronze com campos de
# auditoria.
# ============================================================

# Define modo de gravacao baseado no watermark
mode = "append" if (isinstance(watermark, dict) and watermark.get("last_date")) else "overwrite"

# Grava na tabela bronze
n = save_to_bronze(despesas_lista, "despesas", "/deputados/{id}/despesas", mode=mode)

# Registra status para o resumo final
status_list.append({"tabela": "ft_bronze.despesas", "registros": n})

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

# Verifica se houve despesas processadas
if despesas_lista:
    # Busca o maior ano entre as despesas
    last_ano = max(d.get('ano', 0) for d in despesas_lista)
    # Busca o maior mes daquele ano
    last_mes = max(d.get('mes', 0) for d in despesas_lista if d.get('ano') == last_ano)
    # Atualiza watermark no formato YYYY-MM-01
    set_watermark("despesas", f"{last_ano}-{last_mes:02d}-01")
    # Informa usuario
    print(f"   Watermark: {last_ano}-{last_mes:02d}")

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
