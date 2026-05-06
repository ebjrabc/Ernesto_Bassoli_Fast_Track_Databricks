# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

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

# DBTITLE 1,Definicao de Funcoes e Parametros
# MAGIC %md
# MAGIC # Definicao de Funcoes, Parametros e Variaveis

# COMMAND ----------

# DBTITLE 1,Sobre o comando run
# MAGIC %md
# MAGIC Ao executar o comando `%run ../FUNCOES_GENERICAS`, o Python ira interpretar e executar
# MAGIC o conteudo do arquivo FUNCOES_GENERICAS.py, disponibilizando todas as funcoes de
# MAGIC ingestao, gravacao, controle incremental e validacao de qualidade para uso neste notebook.

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

# DBTITLE 1,Sobre a Lista de Deputados
# MAGIC %md
# MAGIC Na celula abaixo e realizada a busca da lista completa de deputados federais
# MAGIC da Legislatura 57 (atual, 2023-2027). A funcao `fetch_api` cuida automaticamente
# MAGIC da paginacao (a API retorna no maximo 100 por pagina) e das tentativas em caso de erro.

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

# Informa o usuario que a ingestao esta iniciando
print("Ingerindo lista de deputados (Legislatura 57)...")

# Chama a funcao de ingestao para o endpoint de deputados
# Parametros: filtra pela legislatura atual e ordena por nome
deputados_lista = fetch_api(
    endpoint="/deputados",
    params={"idLegislatura": 57, "ordenarPor": "nome"}
)

# Exibe a quantidade de deputados encontrados na API
print(f"   Deputados encontrados: {len(deputados_lista)}")

# COMMAND ----------

# DBTITLE 1,Sobre os Detalhes Individuais
# MAGIC %md
# MAGIC Na celula abaixo, para cada deputado da lista, e feita uma requisicao individual
# MAGIC ao endpoint `/deputados/{id}` para obter informacoes detalhadas como CPF, sexo,
# MAGIC data de nascimento, naturalidade, escolaridade, situacao e dados do gabinete.
# MAGIC O processo exibe progresso a cada 50 deputados processados.

# COMMAND ----------

# DBTITLE 1,Ingere Detalhes Deputados
# ============================================================
# INGESTAO DOS DETALHES DE CADA DEPUTADO
# ============================================================
# Para cada deputado, busca informacoes detalhadas via
# endpoint /deputados/{id}. Campos adicionais incluem:
# cpf, sexo, dataNascimento, naturalidade, escolaridade,
# situacao, gabinete e redes sociais.
# ============================================================

# Informa o usuario que esta iniciando a busca de detalhes
print("Ingerindo detalhes individuais de cada deputado...")

# Lista para acumular os detalhes de cada deputado
deputados_detalhes = []

# Total de deputados para calcular progresso percentual
total = len(deputados_lista)

# Loop: percorre cada deputado da lista para buscar detalhes
for i, dep in enumerate(deputados_lista):
    # Extrai o ID do deputado atual
    dep_id = dep['id']
    
    try:
        # Monta a URL completa do endpoint de detalhes
        url = f"{API_BASE_URL}/deputados/{dep_id}"
        
        # Faz a requisicao GET com timeout configurado
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        
        # Se a resposta foi sucesso (200)
        if r.status_code == 200:
            # Extrai os dados do corpo da resposta JSON
            dados = r.json().get('dados', {})
            # Adiciona os detalhes a lista acumulada
            deputados_detalhes.append(dados)
            
        # Se recebeu rate limiting (429) - muitas requisicoes
        elif r.status_code == 429:
            # Informa o usuario e aguarda 2 segundos
            print(f"  Rate limited ao buscar deputado {dep_id} - aguardando...")
            time.sleep(2)
            
        # Qualquer outro status de erro
        else:
            # Informa o usuario do status inesperado
            print(f"  Status {r.status_code} ao buscar deputado {dep_id}")
            
    # Erro de conexao (rede indisponivel)
    except requests.exceptions.ConnectionError:
        # Registra erro critico no log
        log_api_connection_error(f"/deputados/{dep_id}", Exception("ConnectionError"))
        # Informa o usuario
        print(f"  ERRO DE CONEXAO ao buscar deputado {dep_id}")
        print(f"  Verifique a rede e tente novamente")
        # Interrompe o loop (nao adianta tentar outros sem rede)
        break
        
    # Erro de timeout (API demorou demais)
    except requests.exceptions.Timeout:
        # Informa o usuario
        print(f"  Timeout ao buscar deputado {dep_id}")
        
    # Qualquer outro erro inesperado
    except Exception as e:
        # Informa o usuario com parte da mensagem de erro
        print(f"  Erro inesperado deputado {dep_id}: {str(e)[:60]}")
    
    # A cada 50 deputados, exibe o progresso para acompanhamento
    if (i + 1) % 50 == 0:
        # Calcula e exibe percentual concluido
        print(f"   Progresso: {i+1}/{total} ({(i+1)*100//total}%)")
        # Pequena pausa para nao sobrecarregar a API
        time.sleep(0.5)

# Exibe resumo final da ingestao de detalhes
print(f"   Detalhes obtidos: {len(deputados_detalhes)}/{total}")

# COMMAND ----------

# DBTITLE 1,Gravacao dos Dados
# MAGIC %md
# MAGIC # Gravacao na Camada Bronze

# COMMAND ----------

# DBTITLE 1,Sobre a Gravacao
# MAGIC %md
# MAGIC Na celula abaixo os dados sao gravados em duas tabelas na camada Bronze:
# MAGIC 1. **deputados**: lista basica (id, nome, partido, UF)
# MAGIC 2. **deputados_detalhes**: informacoes completas com flatten das estruturas aninhadas
# MAGIC    (ultimoStatus e gabinete sao "achatados" em colunas individuais)

# COMMAND ----------

# DBTITLE 1,Grava Bronze Deputados
# ============================================================
# GRAVACAO NA CAMADA BRONZE
# ============================================================
# Grava a lista de deputados e seus detalhes na tabela
# bronze. A lista basica e gravada em 'deputados' e os
# detalhes completos em 'deputados_detalhes'.
# O flatten transforma dicionarios aninhados em colunas planas.
# ============================================================

# Grava a lista basica de deputados na tabela bronze
n1 = save_to_bronze(deputados_lista, "deputados", "/deputados")

# Registra o status desta tabela para o resumo final
status_list.append({"tabela": "ft_bronze.deputados", "registros": n1})

# Lista para acumular dados com flatten (estrutura plana)
deputados_flat = []

# Loop: para cada deputado com detalhes, faz o flatten
for d in deputados_detalhes:
    # Cria dicionario apenas com campos simples (sem dicts/listas aninhados)
    flat = {k: v for k, v in d.items() if not isinstance(v, dict) and not isinstance(v, list)}
    
    # Extrai o sub-dicionario 'ultimoStatus' (contem partido atual, situacao, etc)
    ult = d.get('ultimoStatus', {})
    
    # Se existe ultimoStatus, adiciona seus campos com prefixo 'status_'
    if ult:
        for k, v in ult.items():
            # Ignora sub-dicionarios dentro de ultimoStatus
            if not isinstance(v, dict):
                flat[f"status_{k}"] = v
        
        # Extrai o sub-dicionario 'gabinete' dentro de ultimoStatus
        gab = ult.get('gabinete', {})
        
        # Se existe gabinete, adiciona seus campos com prefixo 'gabinete_'
        if gab:
            for k, v in gab.items():
                flat[f"gabinete_{k}"] = v
    
    # Adiciona o registro achatado a lista
    deputados_flat.append(flat)

# Grava os detalhes com flatten na tabela bronze
n2 = save_to_bronze(deputados_flat, "deputados_detalhes", "/deputados/{id}")

# Registra o status desta tabela para o resumo final
status_list.append({"tabela": "ft_bronze.deputados_detalhes", "registros": n2})

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

# DBTITLE 1,Verifica Qualidade
# ============================================================
# VALIDACAO DE QUALIDADE DOS DADOS
# ============================================================
# Verifica completude e integridade dos dados ingeridos.
# Campos obrigatorios: id, nome, siglaPartido, siglaUf.
# Verifica duplicatas pela chave primaria (id).
# ============================================================

# Carrega a tabela recem-gravada para validacao
df_dep = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.deputados")

# Executa validacao de qualidade (nulos e duplicatas)
check_quality(
    df_dep, 
    "deputados", 
    key_columns=["id"],
    critical_columns=["id", "nome", "siglaPartido", "siglaUf"]
)

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

