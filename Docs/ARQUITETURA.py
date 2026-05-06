# Databricks notebook source
# DBTITLE 1,Banner do Projeto
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="150"/>
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Descricao do Notebook
# MAGIC %md
# MAGIC # Pipeline: Documentacao - Arquitetura do Pipeline
# MAGIC
# MAGIC ## Descricao
# MAGIC Este notebook documenta a arquitetura completa do projeto Fast Track.
# MAGIC Contem: diagrama de componentes, 17 tasks com dependencias, runbook operacional,
# MAGIC 10 decisoes tecnicas com justificativa e estrategias de recuperacao de falhas.
# MAGIC
# MAGIC ## Entradas (Fontes de Dados)
# MAGIC | Fonte | Descricao |
# MAGIC |-------|----------|
# MAGIC | `Nenhuma` | Este notebook e apenas documentacao |
# MAGIC
# MAGIC ## Saidas (Tabelas de Destino)
# MAGIC | Tabela | Descricao |
# MAGIC |--------|----------|
# MAGIC | `Nenhuma` | Referencia para operacao e manutencao |
# MAGIC
# MAGIC ## Responsavel
# MAGIC - **Ernesto Bassoli Junior**

# COMMAND ----------

# DBTITLE 1,Definicao de Funcoes e Parametros
# MAGIC %md
# MAGIC # Definicao de Funcoes, Parametros e Variaveis
# MAGIC
# MAGIC Na celula abaixo e apresentado o conteudo principal da documentacao tecnica.

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
log_notebook_start("ARQUITETURA")

# COMMAND ----------

# Databricks notebook source
# DBTITLE 1,Banner
# MAGIC %md
# MAGIC <img src="https://dadosabertos.camara.leg.br/api/v2/imgs/logo_camara.png" width="120"/>
# MAGIC 
# MAGIC # Documentação Técnica — Arquitetura e Runbook
# MAGIC 
# MAGIC **Projeto:** Desafio Final - Upskill Tiller | Engenharia de Dados | T2
# MAGIC 
# MAGIC **Responsável:** Ernesto Bassoli

# COMMAND ----------

# DBTITLE 1,Arquitetura
# MAGIC %md
# MAGIC ## 1. Arquitetura da Solução
# MAGIC 
# MAGIC ### Arquitetura Medalhão (Medallion Architecture)
# MAGIC 
# MAGIC Optou-se pela arquitetura Medalhão por:
# MAGIC - **Separação clara de responsabilidades** entre ingestão (Bronze), transformação (Silver) e análise (Gold)
# MAGIC - **Rastreabilidade completa** via campos de auditoria em cada camada
# MAGIC - **Reprocessamento seguro** com Delta Time Travel em todas as camadas
# MAGIC - **Escalabilidade** para novos endpoints e análises sem impactar camadas anteriores
# MAGIC 
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────┐
# MAGIC │                         API Câmara dos Deputados                        │
# MAGIC │   /deputados  /frentes  /eventos  /votacoes  /proposicoes  /despesas   │
# MAGIC └───────────┬─────────────────────────────────────────────────────────────┘
# MAGIC             │
# MAGIC             ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────────────┐
# MAGIC │  BRONZE (dt0025_dev.ft_bronze)                                          │
# MAGIC │  ─────────────────────────────                                          │
# MAGIC │  • Dados brutos JSON → Delta                                            │
# MAGIC │  • Append-only (preserva histórico)                                     │
# MAGIC │  • Campos auditoria: _ingested_at, _source_endpoint, _batch_id          │
# MAGIC │  • Controle incremental: _control_watermarks                            │
# MAGIC │  • Tabelas: deputados, frentes, frentes_membros, eventos,               │
# MAGIC │    eventos_presenca, votacoes, votos, proposicoes, tramitacoes,          │
# MAGIC │    despesas, orgaos, partidos, legislaturas                              │
# MAGIC └───────────┬─────────────────────────────────────────────────────────────┘
# MAGIC             │
# MAGIC             ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────────────┐
# MAGIC │  SILVER (dt0025_dev.ft_silver)                                          │
# MAGIC │  ──────────────────────────────                                         │
# MAGIC │  • Dados limpos, tipados, deduplicados                                  │
# MAGIC │  • Merge incremental (upsert) via hash de mudança                       │
# MAGIC │  • SCD Type 2 para proposicoes (CDC completo)                           │
# MAGIC │  • Star Schema: fato + dimensões                                        │
# MAGIC │  • Tabelas: dim_deputados, dim_eventos, dim_fornecedores,               │
# MAGIC │    fato_presenca, fato_votos, fato_despesas, proposicoes_scd2            │
# MAGIC └───────────┬─────────────────────────────────────────────────────────────┘
# MAGIC             │
# MAGIC             ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────────────┐
# MAGIC │  GOLD (dt0025_dev.ft_gold)                                              │
# MAGIC │  ─────────────────────────────                                          │
# MAGIC │  • Tabelas analíticas prontas para consumo                              │
# MAGIC │  • Métricas pré-calculadas e scores                                     │
# MAGIC │  • Entregáveis:                                                         │
# MAGIC │    1. Atlas Frentes (Herfindahl, multi-frentes, sobreposição)            │
# MAGIC │    2. Calendário Analítico (densidade, comparativo eleitoral)            │
# MAGIC │    3. Correlação Frentes×Votações (coesão, comparativo partido)          │
# MAGIC │    4. Raio-X Gastos (z-score anomalias, ranking fornecedores)            │
# MAGIC │    5. Auditoria CPIs (timeline, produtividade, legislação derivada)      │
# MAGIC │    6. Monitor Presença (score engajamento, série temporal)               │
# MAGIC └─────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Modelagem
# MAGIC %md
# MAGIC ## 2. Modelagem e Otimização
# MAGIC 
# MAGIC ### Modelos Utilizados
# MAGIC 
# MAGIC | Modelo | Tabela | Justificativa |
# MAGIC |--------|--------|---------------|
# MAGIC | **SCD Type 2** | proposicoes_scd2 | Rastreia evolução completa de PLs. Campos valid_from/valid_to/is_current permitem reconstrução em qualquer ponto no tempo |
# MAGIC | **Star Schema** | fato_despesas + dims | Otimizado para queries analíticas de alto volume com JOINs previsíveis |
# MAGIC | **Fato/Dimensão** | fato_presenca, fato_votos | Grain claro (1 registro por evento atômico), suporta agregações variadas |
# MAGIC | **Gold Aggregated** | score_engajamento, frentes_diversidade | Métricas pré-calculadas para dashboards de baixa latência |
# MAGIC 
# MAGIC ### Otimizações
# MAGIC 
# MAGIC - **Delta Lake**: ACID transactions, time travel, schema evolution
# MAGIC - **Merge incremental**: Apenas registros alterados são processados (hash-based CDC)
# MAGIC - **Partition pruning**: Tabelas grandes particionadas por ano/mês
# MAGIC - **Z-ordering**: Em colunas de filtro frequente (id_deputado, sigla_partido)
# MAGIC - **Surrogate keys (MD5)**: JOINs eficientes entre camadas

# COMMAND ----------

# DBTITLE 1,Carga Incremental
# MAGIC %md
# MAGIC ## 3. Carga Incremental
# MAGIC 
# MAGIC ### Estratégias por Endpoint
# MAGIC 
# MAGIC | Endpoint | Estratégia | Controle |
# MAGIC |----------|-----------|----------|
# MAGIC | /votacoes | Offset por ID | Watermark do último ID processado |
# MAGIC | /eventos | Data watermark | Última dataHoraInicio |
# MAGIC | /proposicoes | Data apresentação | dataApresentacaoInicio incremental |
# MAGIC | /despesas | Ano + Mês | Último ano/mês gravado |
# MAGIC | /frentes, /orgaos | Full refresh | Dados estáveis, refresh semanal |
# MAGIC 
# MAGIC ### Tabela de Controle
# MAGIC 
# MAGIC ```sql
# MAGIC -- dt0025_dev.ft_bronze._control_watermarks
# MAGIC -- table_name | last_id | last_date | last_run
# MAGIC -- Cada notebook atualiza seu watermark após gravação bem-sucedida
# MAGIC ```
# MAGIC 
# MAGIC ### Detecção de Mudanças (CDC)
# MAGIC 
# MAGIC - Hash MD5 do payload completo em cada registro bronze
# MAGIC - Merge condicional: UPDATE apenas se hash difere
# MAGIC - SCD Type 2: registro anterior recebe valid_to, novo é inserido com is_current=true

# COMMAND ----------

# DBTITLE 1,Resiliência
# MAGIC %md
# MAGIC ## 4. Resiliência e Recuperação
# MAGIC 
# MAGIC ### Estratégias
# MAGIC 
# MAGIC | Cenário | Ação |
# MAGIC |---------|------|
# MAGIC | API indisponível | Retry com backoff exponencial (3 tentativas, 1s/2s/4s) |
# MAGIC | Rate limiting (429) | Espera 2^n segundos antes de retry |
# MAGIC | Timeout | Timeout de 30s por request, retry automático |
# MAGIC | Dados parciais | Watermark só atualiza APÓS gravação bem-sucedida |
# MAGIC | Falha no meio do batch | Reprocessa do último watermark (idempotente) |
# MAGIC | Corrupção de dados | Delta Time Travel: RESTORE TABLE AS OF VERSION |
# MAGIC 
# MAGIC ### Replay e Reprocessamento
# MAGIC 
# MAGIC ```python
# MAGIC # Para reprocessar desde uma data específica:
# MAGIC # 1. Atualiza watermark manualmente
# MAGIC spark.sql("DELETE FROM ft_bronze._control_watermarks WHERE table_name = 'eventos'")
# MAGIC # 2. Re-executa notebook
# MAGIC # 3. Dados são re-ingeridos com append (dedup no Silver via merge)
# MAGIC ```
# MAGIC 
# MAGIC ### Monitoramento
# MAGIC 
# MAGIC - Tabela `sla_metricas_streaming`: latência, volume, erros por batch
# MAGIC - Alertas quando latência > 60s ou erros > 0
# MAGIC - Dashboard SLA com gráficos de tendência

# COMMAND ----------

# DBTITLE 1,Runbook
# MAGIC %md
# MAGIC ## 5. Runbook de Incidentes
# MAGIC 
# MAGIC ### Pipeline de Execução
# MAGIC 
# MAGIC ```
# MAGIC JOB: fast_track_pipeline (agendado diário 06:00)
# MAGIC ├── Task 1: Bronze/bronze_orgaos_partidos.py (dimensões)
# MAGIC ├── Task 2: Bronze/bronze_deputados.py
# MAGIC ├── Task 3: Bronze/bronze_frentes.py
# MAGIC ├── Task 4: Bronze/bronze_eventos.py
# MAGIC ├── Task 5: Bronze/bronze_votacoes.py
# MAGIC ├── Task 6: Bronze/bronze_proposicoes.py
# MAGIC ├── Task 7: Bronze/bronze_despesas.py
# MAGIC ├── Task 8: Silver/silver_deputados.py (depende: 2)
# MAGIC ├── Task 9: Silver/silver_eventos_votacoes.py (depende: 4, 5)
# MAGIC ├── Task 10: Silver/silver_despesas.py (depende: 7, 8)
# MAGIC ├── Task 11: Silver/silver_proposicoes_scd2.py (depende: 6)
# MAGIC ├── Task 12: Gold/gold_atlas_frentes.py (depende: 3, 8)
# MAGIC ├── Task 13: Gold/gold_calendario_eventos.py (depende: 9)
# MAGIC ├── Task 14: Gold/gold_correlacao_frentes_votacoes.py (depende: 9, 12)
# MAGIC ├── Task 15: Gold/gold_raio_x_gastos.py (depende: 10)
# MAGIC ├── Task 16: Gold/gold_auditoria_cpis.py (depende: 9, 11)
# MAGIC └── Task 17: Gold/gold_monitor_presenca.py (depende: 9)
# MAGIC 
# MAGIC JOB: streaming_votacoes (agendado cada 10 min)
# MAGIC └── Task 1: Streaming/streaming_votacoes.py
# MAGIC ```
# MAGIC 
# MAGIC ### Procedimentos de Recuperação
# MAGIC 
# MAGIC | Incidente | Procedimento |
# MAGIC |-----------|-------------|
# MAGIC | Task falhou | Verificar logs → corrigir → re-executar task individual |
# MAGIC | Dados incorretos Silver | RESTORE TABLE → fix → re-merge |
# MAGIC | API fora por horas | Aguardar → pipeline retoma do watermark automaticamente |
# MAGIC | Duplicatas na Gold | Verificar merge keys → fix dedup → rebuild Gold |
# MAGIC | Performance degradada | Verificar OPTIMIZE/VACUUM → Z-ORDER em chaves de filtro |

# COMMAND ----------

# DBTITLE 1,Decisões Técnicas
# MAGIC %md
# MAGIC ## 6. Registro de Decisões Técnicas
# MAGIC 
# MAGIC | # | Decisão | Motivo |
# MAGIC |---|---------|--------|
# MAGIC | 1 | PySpark nativo (sem pandas) | Performance em escala, paralelismo, integração Delta |
# MAGIC | 2 | Medallion Architecture | Padrão Databricks, separação de concerns, reprocessamento |
# MAGIC | 3 | Delta Lake | ACID, time travel, merge nativo, schema evolution |
# MAGIC | 4 | Watermark em tabela Delta | Persistente, auditável, permite reset manual |
# MAGIC | 5 | Hash MD5 para CDC | Detecção eficiente de mudanças sem comparar campo a campo |
# MAGIC | 6 | SCD2 manual (não DLT) | Controle fino sobre valid_from/to, sem dependência de DLT |
# MAGIC | 7 | Z-score para anomalias | Estatisticamente fundamentado, interpretável, por grupo |
# MAGIC | 8 | Herfindahl para diversidade | Índice acadêmico consolidado, comparável entre frentes |
# MAGIC | 9 | Score composto ponderado | Presença (40%) + Votações (60%) reflete importância relativa |
# MAGIC | 10 | API polling (não webhook) | API da Câmara não oferece webhooks; micro-batch é alternativa |
# MAGIC 
# MAGIC ### Relacionamentos entre Tabelas
# MAGIC 
# MAGIC ```
# MAGIC dim_deputados.id_deputado ──┬── fato_presenca.id_deputado
# MAGIC                             ├── fato_votos.id_deputado
# MAGIC                             ├── fato_despesas.id_deputado
# MAGIC                             └── atlas_frentes.id_deputado
# MAGIC 
# MAGIC dim_eventos.id_evento ──────┬── fato_presenca.id_evento
# MAGIC                             └── calendario_analitico.id_evento
# MAGIC 
# MAGIC proposicoes_scd2.id_proposicao ── votacoes.proposicaoObjeto
# MAGIC 
# MAGIC atlas_frentes.id_frente ────┬── frentes_diversidade.id_frente
# MAGIC                             ├── coesao_frentes.id_frente
# MAGIC                             └── frentes_sobreposicao.frente_a_id/frente_b_id
# MAGIC ```
