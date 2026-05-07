# Gêmeo Digital Legislativo

> Plataforma de análise político-digital dos deputados federais brasileiros e laboratório de engenharia de dados moderno. Combina dados da Câmara dos Deputados, redes sociais e Google Trends para gerar inteligência legislativa acionável — com arquitetura **Medallion + Delta Lake**, orquestração via **Apache Airflow**, processamento distribuído com **PySpark**, princípios **SOLID**, containers **Docker** e deploy **Serverless na AWS**.

---

## Sumário

- [Visão geral e propósito](#visão-geral-e-propósito)
- [Princípios SOLID aplicados](#princípios-solid-aplicados)
- [Stack tecnológica](#stack-tecnológica)
- [Arquitetura completa](#arquitetura-completa)
- [Estrutura de pastas e arquivos](#estrutura-de-pastas-e-arquivos)
- [Pré-requisitos](#pré-requisitos)
- [Passo 1 — Configurar o ambiente](#passo-1--configurar-o-ambiente)
- [Passo 2 — Construir as interfaces SOLID](#passo-2--construir-as-interfaces-solid)
- [Passo 3 — Implementar os repositórios](#passo-3--implementar-os-repositórios)
- [Passo 4 — Implementar os connectors](#passo-4--implementar-os-connectors)
- [Passo 5 — Subir o ambiente Docker](#passo-5--subir-o-ambiente-docker)
- [Passo 6 — Pipeline Bronze com PySpark](#passo-6--pipeline-bronze-com-pyspark)
- [Passo 7 — Pipeline Silver com Delta Lake](#passo-7--pipeline-silver-com-delta-lake)
- [Passo 8 — Pipeline Gold com PySpark](#passo-8--pipeline-gold-com-pyspark)
- [Passo 9 — Orquestrar com Apache Airflow](#passo-9--orquestrar-com-apache-airflow)
- [Passo 10 — Ciência de Dados](#passo-10--ciência-de-dados)
- [Passo 11 — IA local (NLP + RAG)](#passo-11--ia-local-nlp--rag)
- [Passo 12 — Dashboard e API](#passo-12--dashboard-e-api)
- [Passo 13 — Deploy na AWS](#passo-13--deploy-na-aws)
- [Docker — referência completa](#docker--referência-completa)
- [Comandos de referência (Makefile)](#comandos-de-referência-makefile)
- [Pacotes utilizados](#pacotes-utilizados)
- [Variáveis de ambiente](#variáveis-de-ambiente)
- [Equivalências local ↔ AWS](#equivalências-local--aws)
- [Roadmap](#roadmap)

---

## Visão geral e propósito

O **Gêmeo Digital Legislativo** é simultaneamente um produto de dados e um laboratório de engenharia moderna. Ele responde perguntas como:

| Pergunta | Técnica utilizada |
|---|---|
| Quem são os deputados mais influentes digitalmente? | Score composto com pesos configuráveis |
| Votações em pautas específicas afetam o engajamento? | OLS e GLM Poisson (statsmodels) |
| Como o engajamento vai evoluir nos próximos 30 dias? | ARIMA automático (pmdarima) + Prophet |
| Quais grupos de deputados existem por perfil? | K-Means + DBSCAN + PCA (scikit-learn) |
| O que os deputados dizem e com qual sentimento? | BERTimbau (HuggingFace) em PT-BR |
| Posso fazer perguntas sobre os discursos? | RAG com ChromaDB + Ollama local |

Como **laboratório**, o projeto cobre na prática:

- **Engenharia de dados**: arquitetura Medallion, transações ACID, time travel, schema evolution (Delta Lake)
- **Processamento distribuído**: PySpark com DataFrames e Spark SQL
- **Orquestração**: DAGs Airflow com sensores, retries, SLAs e alertas
- **Design de software**: princípios SOLID com Protocols Python
- **Containerização**: Docker Compose para ambiente local, imagens Lambda para AWS
- **Serverless**: AWS Lambda, S3, MWAA, EMR Serverless, ECS Fargate

---

## Princípios SOLID aplicados

O design inteiro gira em torno de **interfaces** definidas em `src/interfaces/`. Nenhuma camada depende de implementações concretas — isso é o que permite trocar DuckDB por Delta Lake, ChromaDB por OpenSearch, e scripts Python por DAGs Airflow sem tocar no código de modelos ou dashboard.

| Princípio | Aplicação no projeto |
|---|---|
| **S** — Single Responsibility | Cada Spark job faz uma coisa: `bronze_job.py` só ingere, `silver_job.py` só transforma, cada modelo DS só roda seu algoritmo |
| **O** — Open/Closed | Adicionar um conector novo (ex: Twitter) ou modelo novo (ex: XGBoost) cria um arquivo novo que implementa a interface — sem modificar código existente |
| **L** — Liskov Substitution | `DeltaRepository`, `S3Repository` e `InMemoryRepository` são intercambiáveis — qualquer código que aceita `IRepository` funciona com os três |
| **I** — Interface Segregation | `IReadRepository` e `IWriteRepository` são contratos separados — o dashboard depende só de leitura |
| **D** — Dependency Inversion | Modelos DS e jobs Spark recebem `IRepository` via injeção — a `RepositoryFactory` decide a implementação concreta via `DEPLOY_ENV` |

### Exemplo — adicionando um novo conector (Open/Closed)

```python
# src/connectors/twitter_connector.py
# Nenhum arquivo existente é modificado

from src.interfaces.connector import IConnector
import polars as pl

class TwitterConnector(IConnector):
    def fetch_raw(self, deputy_id: str) -> pl.DataFrame:
        # ... lógica de extração
        return df
```

### Exemplo — troca de storage transparente (Dependency Inversion)

```python
# src/repositories/factory.py
import os

def get_repository():
    env = os.getenv("DEPLOY_ENV", "local")
    match env:
        case "spark":
            from src.repositories.delta_repository import DeltaRepository
            return DeltaRepository(base_path="data/")
        case "aws":
            from src.repositories.s3_repository import S3Repository
            return S3Repository(bucket=os.getenv("S3_BUCKET"))
        case _:
            from src.repositories.local_repository import LocalParquetRepository
            return LocalParquetRepository(base_path="data/")

# Os Spark jobs e modelos DS nunca importam DeltaRepository diretamente
# def run_job(reader: IReadRepository, writer: IWriteRepository): ...
```

---

## Stack tecnológica

### Núcleo de dados

| Tecnologia | Papel | Substitui (local → AWS) |
|---|---|---|
| **PySpark 3.5** | Processamento distribuído Bronze→Silver→Gold | Polars (local leve) → EMR Serverless (AWS) |
| **Delta Lake 3.1** | ACID, time travel, schema evolution, upsert | Parquet simples + DuckDB → Delta em S3 |
| **Apache Airflow 2.9** | Orquestração de DAGs, retry, SLA, alertas | Scripts manuais → MWAA (AWS) |
| **Polars** | Processamento leve fora do Spark (connectors, testes) | — |
| **Pydantic v2** | Validação de schema na ingestão | — |

### Ciência de Dados

| Tecnologia | Papel |
|---|---|
| **statsmodels** | OLS, GLM Poisson/Binomial Negativo, decomposição STL |
| **scikit-learn** | K-Means, DBSCAN, PCA, StandardScaler |
| **pmdarima** | ARIMA automático (auto_arima) |
| **prophet** | Séries temporais com eventos externos |
| **scipy** | Testes estatísticos (Shapiro-Wilk, Breusch-Pagan) |

### IA local

| Tecnologia | Papel | Substitui na AWS |
|---|---|---|
| **BERTimbau** (HuggingFace) | Sentimento em PT-BR | — |
| **sentence-transformers** | Embeddings para RAG | — |
| **ChromaDB** | Vector store local | OpenSearch Serverless |
| **LangChain** | Pipeline RAG | — |
| **Ollama** | LLM local (Llama3, Mistral) | Bedrock (Claude Haiku) |

### Interface e infra

| Tecnologia | Papel | Substitui na AWS |
|---|---|---|
| **Streamlit** | Dashboard web | ECS Fargate |
| **FastAPI + mangum** | API REST | API Gateway + Lambda |
| **Docker Compose** | Ambiente local completo | — |
| **AWS SAM** | Deploy serverless | — |

---

## Arquitetura completa

### Visão local (laboratório)

```
┌─────────────────────────────────────────────────────────┐
│  Apache Airflow  (localhost:8080)                       │
│                                                         │
│  ingest_dag ──► transform_dag ──► models_dag           │
│  (diário)        (sensor Bronze)   (sensor Gold)        │
└──────────┬──────────────┬──────────────┬───────────────┘
           │ SparkSubmit  │ SparkSubmit  │ PythonOperator
           ▼              ▼              ▼
    ┌──────────────────────────────────────────────┐
    │  PySpark  (spark-master:7077)                │
    │  bronze_job · silver_job · gold_job          │
    └──────────────────┬───────────────────────────┘
                       │ lê / escreve
                       ▼
    ┌──────────────────────────────────────────────┐
    │  Delta Lake  (data/)                         │
    │  bronze/ · silver/ · gold/                   │
    │  ACID · time travel · schema evolution       │
    └──────────────────┬───────────────────────────┘
                       │ toPandas() / IReadRepository
           ┌───────────┴───────────┐
           ▼                       ▼
  ┌─────────────────┐    ┌──────────────────────┐
  │  Modelos DS     │    │  Streamlit :8501      │
  │  OLS·GLM·ARIMA  │    │  FastAPI   :8000      │
  │  Prophet·NLP    │    │  Ollama    :11434     │
  │  RAG·Clusters   │    └──────────────────────┘
  └─────────────────┘
```

### Visão AWS (produção)

```
MWAA (Managed Airflow)
    │
    ├─► EMR Serverless ──► S3 Bronze (Delta)
    │                 ──► S3 Silver (Delta, MERGE INTO)
    │                 ──► S3 Gold   (Delta, Z-ORDER)
    │
    └─► Lambda (modelos DS leves)
        Lambda (NLP / container image)
            │
            ▼
        S3 Gold (features prontas)
            │
    ┌───────┴────────┐
    ▼                ▼
ECS Fargate    API Gateway
(Streamlit)    + Lambda
               (FastAPI+mangum)
```

### O que muda entre local e AWS

| Componente | Local (laboratório) | AWS (produção) |
|---|---|---|
| Orquestração | Airflow local (Docker) | MWAA |
| Processamento Spark | Spark standalone (Docker) | EMR Serverless |
| Storage Delta | `data/` local | S3 + Delta (mesmo formato) |
| Vector store | ChromaDB (arquivo) | OpenSearch Serverless |
| LLM | Ollama local | Bedrock (Claude Haiku) |
| Dashboard | `streamlit run` local | ECS Fargate |
| API | FastAPI + uvicorn | API Gateway + Lambda |
| Lambdas leves | Scripts locais | AWS Lambda (ZIP) |
| Lambdas pesadas | Scripts locais | AWS Lambda (container image) |

> A troca é transparente via `RepositoryFactory`. Setando `DEPLOY_ENV=aws` ou `DEPLOY_ENV=spark`, a factory escolhe a implementação correta sem mudar nenhum código de negócio.

---

## Estrutura de pastas e arquivos

```
gemeo-digital-legislativo/
│
├── src/
│   ├── interfaces/                      # Contratos SOLID — núcleo do design
│   │   ├── __init__.py
│   │   ├── connector.py                 # IConnector: fetch_raw() → pl.DataFrame
│   │   ├── repository.py                # IReadRepository · IWriteRepository · IDeltaRepository
│   │   ├── model_runner.py              # IModelRunner: run(df) → ModelResult
│   │   └── embedder.py                  # IEmbedder · IVectorStore
│   │
│   ├── connectors/                      # Implementações de IConnector (SRP + OCP)
│   │   ├── camara_connector.py          # API Câmara: deputados, votos, proposições, discursos
│   │   ├── social_connector.py          # Instagram via Apify SDK
│   │   └── trends_connector.py          # Google Trends via pytrends
│   │
│   ├── repositories/                    # Implementações de IRepository (LSP)
│   │   ├── delta_repository.py          # Delta Lake: ACID, merge_into(), time_travel()
│   │   ├── s3_repository.py             # S3 + Delta em produção AWS
│   │   ├── local_repository.py          # DuckDB + Parquet (testes leves sem Spark)
│   │   ├── in_memory_repository.py      # InMemory para testes unitários (zero I/O)
│   │   └── factory.py                   # RepositoryFactory via DEPLOY_ENV
│   │
│   ├── spark_jobs/                      # Jobs PySpark — processamento distribuído
│   │   ├── bronze_job.py                # Lê connectors → schema enforcement → Delta Bronze
│   │   ├── silver_job.py                # Delta Bronze → validação → MERGE INTO Delta Silver
│   │   └── gold_job.py                  # Delta Silver → features → Delta Gold (Z-ORDER)
│   │
│   ├── models/                          # Modelos DS — todos implementam IModelRunner
│   │   ├── eda.py                       # AED: distribuições, correlações, outliers
│   │   ├── linear_models.py             # OLS + GLM Poisson/Binomial Negativo
│   │   ├── time_series.py               # STL + ARIMA (pmdarima) + Prophet
│   │   ├── clustering.py                # K-Means + DBSCAN + PCA
│   │   ├── nlp_sentiment.py             # BERTimbau + VADER fallback
│   │   ├── rag_indexer.py               # ChromaDB ↔ OpenSearch via IVectorStore
│   │   └── score.py                     # Score de influência composto
│   │
│   ├── api/                             # FastAPI + mangum (ISP — endpoints separados)
│   │   ├── main.py                      # App FastAPI + Mangum handler
│   │   ├── deputies.py                  # GET /deputies · GET /deputies/{id}
│   │   ├── scores.py                    # GET /scores/ranking
│   │   ├── timeseries.py                # GET /timeseries/{id}
│   │   └── chat.py                      # POST /chat — RAG streaming
│   │
│   └── dashboard/                       # Streamlit — lê via IReadRepository
│       ├── dashboard.py                 # Entry point + navegação por abas
│       ├── pages/
│       │   ├── visao_geral.py           # Mapa coroplético + ranking top 20
│       │   ├── perfil.py                # Perfil individual com histórico
│       │   ├── series_temporais.py      # Prophet interativo + previsão 30 dias
│       │   ├── modelos.py               # Coeficientes OLS/GLM + diagnóstico
│       │   ├── clusters.py              # Scatter PCA + descrição dos segmentos
│       │   └── chat_rag.py              # Chat RAG com fontes citadas
│       └── components/
│           ├── cards.py                 # Card de deputado (foto + métricas)
│           └── charts.py                # Wrappers Plotly reutilizáveis
│
├── dags/                                # DAGs Apache Airflow
│   ├── ingest_dag.py                    # Cron diário: 3 connectors em paralelo → Bronze
│   ├── transform_dag.py                 # Sensor Bronze → Silver → Gold (sequencial)
│   └── models_dag.py                    # Sensor Gold → 5 modelos em paralelo
│
├── notebooks/                           # Exploração e prototipagem
│   ├── 01_AED.ipynb                     # Análise exploratória inicial
│   ├── 02_segmentacao.ipynb             # Experimentos K-Means e DBSCAN
│   ├── 03_series_temporais.ipynb        # ARIMA e Prophet com diagnóstico
│   ├── 04_modelos_lineares.ipynb        # OLS, GLM e testes de hipótese
│   └── 05_nlp_sentimento.ipynb          # BERTimbau avaliação e erros
│
├── tests/
│   ├── test_connectors.py               # Mock IConnector com httpx.MockTransport
│   ├── test_models.py                   # Smoke tests com dados sintéticos
│   ├── test_repositories.py             # InMemoryRepository — zero I/O
│   └── test_spark_jobs.py               # SparkSession local para testes unitários
│
├── infra/
│   ├── airflow/
│   │   ├── Dockerfile                   # Imagem Airflow customizada com PySpark
│   │   ├── requirements.txt             # Providers: apache-airflow-providers-apache-spark
│   │   └── airflow.cfg                  # Configurações do Airflow
│   ├── lambdas/
│   │   ├── ingest/requirements.txt      # Deps leves — deploy ZIP
│   │   ├── models/
│   │   │   ├── Dockerfile               # Lambda container: prophet + statsmodels
│   │   │   └── requirements.txt
│   │   └── nlp/
│   │       ├── Dockerfile               # Lambda container: torch + transformers
│   │       └── requirements.txt
│   └── .github/
│       └── workflows/
│           └── deploy.yml               # CI: test → lint → docker build → sam deploy
│
├── data/                                # Ignorado pelo git
│   ├── bronze/                          # Delta Lake Bronze (_delta_log/ + parquet)
│   ├── silver/                          # Delta Lake Silver
│   └── gold/                            # Delta Lake Gold
│
├── config/
│   └── settings.py                      # Configurações centralizadas via pydantic-settings
│
├── template.yaml                        # AWS SAM — Lambdas leves (ZIP) e pesadas (image)
├── Makefile                             # Todos os comandos do projeto
├── pyproject.toml                       # Deps por grupo: [spark] [models] [nlp] [dashboard]
├── Dockerfile                           # Dashboard Streamlit para ECS Fargate
├── docker-compose.yml                   # Ambiente local: Airflow + Spark + Ollama + serviços
├── .dockerignore
├── .env.example
└── README.md
```

---

## Pré-requisitos

| Requisito | Versão | Observação |
|---|---|---|
| Python | 3.11+ | Recomendado 3.12 |
| Java | 11+ | **Obrigatório para PySpark** — `java -version` |
| Docker | 24+ | Obrigatório para Airflow e ambiente completo |
| Docker Compose | 2.x | Incluído no Docker Desktop |
| uv | 0.4+ | Gerenciador de pacotes Python |
| RAM disponível | 8GB+ | Spark + Airflow + Ollama juntos |
| AWS CLI | 2.x | Apenas para deploy na AWS |
| AWS SAM CLI | 1.x | Apenas para deploy na AWS |
| Apify API Key | — | Gratuita em apify.com |

### Verificar pré-requisitos

```bash
java -version        # deve retornar 11.x ou superior
docker --version     # deve retornar 24.x ou superior
python --version     # deve retornar 3.11.x ou superior
uv --version
```

### Instalar Java 11 (se necessário)

```bash
# Ubuntu/Debian
sudo apt install openjdk-11-jdk

# macOS com Homebrew
brew install openjdk@11

# Windows — baixar em adoptium.net
```

---

## Passo 1 — Configurar o ambiente

### 1.1 Clonar e inicializar

```bash
git clone https://github.com/seu-usuario/gemeo-digital-legislativo.git
cd gemeo-digital-legislativo
```

### 1.2 Instalar dependências base

```bash
pip install uv
uv sync                          # instala dependências base
uv sync --group spark            # adiciona PySpark + Delta Lake
uv sync --group models           # adiciona statsmodels + prophet + pmdarima
uv sync --group nlp              # adiciona torch + transformers (pesado, ~2GB)
uv sync --group dashboard        # adiciona Streamlit + Plotly
```

> Dica: instale os grupos progressivamente conforme avança nas fases.

### 1.3 Configurar variáveis de ambiente

```bash
cp .env.example .env
# Edite .env com seu editor favorito
# Mínimo necessário para começar: APIFY_API_KEY e DEPLOY_ENV=spark
```

### 1.4 Verificar a instalação do Spark

```bash
uv run python -c "from pyspark.sql import SparkSession; print(SparkSession.builder.getOrCreate())"
# Deve imprimir: SparkSession - hive (ou in-memory)
```

### 1.5 Configurar pyproject.toml com grupos

```toml
[project]
name = "gemeo-digital-legislativo"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "polars>=1.0",
    "pydantic>=2.0",
    "pydantic-settings>=2.0",
    "httpx>=0.27",
    "python-dotenv>=1.0",
    "apify-client>=1.7",
    "pytrends>=4.9",
]

[project.optional-dependencies]
spark   = ["pyspark>=3.5", "delta-spark>=3.1"]
models  = ["statsmodels>=0.14", "scikit-learn>=1.4", "prophet>=1.1",
           "pmdarima>=2.0", "scipy>=1.13"]
nlp     = ["torch>=2.2", "transformers>=4.40",
           "sentence-transformers>=3.0", "chromadb>=0.5",
           "langchain>=0.2", "langchain-ollama>=0.1"]
dashboard = ["streamlit>=1.35", "plotly>=5.22", "altair>=5.3",
             "fastapi>=0.111", "uvicorn>=0.29", "mangum>=0.17"]
dev     = ["pytest>=8.0", "pytest-cov>=5.0", "ruff>=0.4",
           "jupyterlab>=4.0", "moto>=5.0"]
```

---

## Passo 2 — Construir as interfaces SOLID

> Este passo cria os contratos que toda a aplicação depende. Nenhuma implementação concreta aqui — apenas `Protocol` do Python.

### 2.1 IConnector

```python
# src/interfaces/connector.py
from typing import Protocol
import polars as pl

class IConnector(Protocol):
    """Contrato para qualquer fonte de dados externa. (DIP + OCP)"""

    def fetch_raw(self, entity_id: str) -> pl.DataFrame:
        """Coleta dados brutos para uma entidade. Nunca transforma."""
        ...

    def fetch_all(self) -> pl.DataFrame:
        """Coleta dados de todas as entidades disponíveis."""
        ...
```

### 2.2 IRepository (segregado — ISP)

```python
# src/interfaces/repository.py
from typing import Protocol
import polars as pl

class IReadRepository(Protocol):
    """Contrato de leitura — o dashboard só precisa disso."""
    def read(self, table: str) -> pl.DataFrame: ...
    def query(self, sql: str) -> pl.DataFrame: ...

class IWriteRepository(Protocol):
    """Contrato de escrita — os jobs Spark usam isso."""
    def write(self, df: pl.DataFrame, table: str, mode: str = "append") -> None: ...
    def merge_into(self, df: pl.DataFrame, table: str, on: list[str]) -> None: ...

class IDeltaRepository(IReadRepository, IWriteRepository, Protocol):
    """Contrato completo com funcionalidades Delta Lake."""
    def time_travel(self, table: str, version: int) -> pl.DataFrame: ...
    def history(self, table: str) -> pl.DataFrame: ...
    def optimize(self, table: str, z_order_by: list[str] | None = None) -> None: ...
    def vacuum(self, table: str, retention_hours: int = 168) -> None: ...
```

### 2.3 IModelRunner

```python
# src/interfaces/model_runner.py
from dataclasses import dataclass, field
from typing import Protocol, Any
import polars as pl

@dataclass
class ModelResult:
    name: str
    metrics: dict[str, float] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)
    timestamp: str = ""

class IModelRunner(Protocol):
    """Todos os modelos DS implementam este contrato. (LSP + OCP)"""
    def run(self, df: pl.DataFrame) -> ModelResult: ...
```

### 2.4 Testar as interfaces com InMemoryRepository

```bash
uv run pytest tests/test_repositories.py -v
# Deve passar sem nenhuma chamada de I/O real
```

---

## Passo 3 — Implementar os repositórios

### 3.1 DeltaRepository (principal)

```python
# src/repositories/delta_repository.py
from pyspark.sql import SparkSession
from delta import DeltaTable
import polars as pl
from src.interfaces.repository import IDeltaRepository

class DeltaRepository:
    """Implementa IDeltaRepository com Delta Lake + PySpark. (LSP)"""

    def __init__(self, base_path: str):
        self.base_path = base_path
        self._spark = (
            SparkSession.builder
            .appName("GemeoDigital")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )

    def _path(self, table: str) -> str:
        return f"{self.base_path}/{table}"

    def read(self, table: str) -> pl.DataFrame:
        sdf = self._spark.read.format("delta").load(self._path(table))
        return pl.from_pandas(sdf.toPandas())

    def write(self, df: pl.DataFrame, table: str, mode: str = "append") -> None:
        sdf = self._spark.createDataFrame(df.to_pandas())
        sdf.write.format("delta").mode(mode).save(self._path(table))

    def merge_into(self, df: pl.DataFrame, table: str, on: list[str]) -> None:
        """Upsert ACID — insere novos, atualiza existentes. Nunca duplica."""
        sdf = self._spark.createDataFrame(df.to_pandas())
        condition = " AND ".join(f"t.{c} = s.{c}" for c in on)
        DeltaTable.forPath(self._spark, self._path(table)) \
            .alias("t").merge(sdf.alias("s"), condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    def time_travel(self, table: str, version: int) -> pl.DataFrame:
        """Lê qualquer versão histórica da tabela."""
        sdf = (self._spark.read.format("delta")
               .option("versionAsOf", version)
               .load(self._path(table)))
        return pl.from_pandas(sdf.toPandas())

    def history(self, table: str) -> pl.DataFrame:
        """Retorna o log de todas as operações na tabela."""
        dt = DeltaTable.forPath(self._spark, self._path(table))
        return pl.from_pandas(dt.history().toPandas())

    def optimize(self, table: str, z_order_by: list[str] | None = None) -> None:
        """Compacta arquivos pequenos. Z-ORDER melhora performance de queries."""
        dt = DeltaTable.forPath(self._spark, self._path(table))
        if z_order_by:
            cols = ", ".join(z_order_by)
            self._spark.sql(
                f"OPTIMIZE delta.`{self._path(table)}` ZORDER BY ({cols})"
            )
        else:
            dt.optimize().executeCompaction()

    def vacuum(self, table: str, retention_hours: int = 168) -> None:
        """Remove arquivos antigos não mais referenciados. Padrão: 7 dias."""
        self._spark.sql(
            f"VACUUM delta.`{self._path(table)}` RETAIN {retention_hours} HOURS"
        )
```

### 3.2 RepositoryFactory

```python
# src/repositories/factory.py
import os
from src.interfaces.repository import IDeltaRepository, IReadRepository, IWriteRepository

def get_repository() -> IDeltaRepository:
    env = os.getenv("DEPLOY_ENV", "spark")
    match env:
        case "aws":
            from src.repositories.s3_repository import S3Repository
            return S3Repository(
                bucket=os.getenv("S3_BUCKET"),
                region=os.getenv("AWS_REGION", "us-east-1")
            )
        case "local":
            from src.repositories.local_repository import LocalParquetRepository
            return LocalParquetRepository(base_path="data/")
        case _:  # "spark" (padrão)
            from src.repositories.delta_repository import DeltaRepository
            return DeltaRepository(base_path="data/")
```

### 3.3 Testar os repositórios

```bash
uv run pytest tests/test_repositories.py -v
# InMemoryRepository: deve passar em <1s, sem nenhuma SparkSession
```

---

## Passo 4 — Implementar os connectors

### 4.1 CamaraConnector

```python
# src/connectors/camara_connector.py
import httpx
import polars as pl
from src.interfaces.connector import IConnector

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

class CamaraConnector:
    """Coleta dados públicos da API da Câmara dos Deputados. (SRP)"""

    def __init__(self):
        self._client = httpx.Client(base_url=BASE_URL, timeout=30)

    def fetch_all(self) -> pl.DataFrame:
        """Retorna todos os deputados em exercício."""
        resp = self._client.get("/deputados", params={"itens": 513})
        resp.raise_for_status()
        return pl.DataFrame(resp.json()["dados"])

    def fetch_raw(self, entity_id: str) -> pl.DataFrame:
        """Retorna dados detalhados de um deputado."""
        resp = self._client.get(f"/deputados/{entity_id}")
        resp.raise_for_status()
        return pl.DataFrame([resp.json()["dados"]])

    def fetch_votes(self, deputy_id: str) -> pl.DataFrame:
        """Retorna histórico de votações do deputado."""
        resp = self._client.get(
            f"/deputados/{deputy_id}/votacoes",
            params={"itens": 100, "ordem": "DESC", "ordenarPor": "dataHoraVoto"}
        )
        resp.raise_for_status()
        return pl.DataFrame(resp.json().get("dados", []))

    def fetch_speeches(self, deputy_id: str) -> pl.DataFrame:
        """Retorna discursos em plenário."""
        resp = self._client.get(
            f"/deputados/{deputy_id}/discursos",
            params={"itens": 50, "ordem": "DESC"}
        )
        resp.raise_for_status()
        return pl.DataFrame(resp.json().get("dados", []))
```

### 4.2 Testar os connectors com mocks

```bash
uv run pytest tests/test_connectors.py -v
# Deve passar sem nenhuma chamada real à internet
```

---

## Passo 5 — Subir o ambiente Docker

> A partir daqui, o ambiente Docker é o coração do laboratório. Sobe Airflow, Spark, serviços e Ollama de uma vez.

### 5.1 docker-compose.yml

```yaml
# docker-compose.yml
x-airflow-common: &airflow-common
  image: gemeo-airflow:latest
  build:
    context: .
    dockerfile: infra/airflow/Dockerfile
  env_file: .env
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    DEPLOY_ENV: spark
    JAVA_HOME: /usr/lib/jvm/java-11-openjdk-amd64
  volumes:
    - ./dags:/opt/airflow/dags
    - ./src:/opt/airflow/src
    - ./data:/opt/airflow/data
    - ./config:/opt/airflow/config
  depends_on:
    postgres:
      condition: service_healthy

services:

  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      timeout: 5s
      retries: 5

  airflow-init:
    <<: *airflow-common
    command: >
      bash -c "airflow db migrate &&
               airflow users create --username admin --password admin
               --firstname Admin --lastname User --role Admin
               --email admin@gemeo.local"

  airflow-webserver:
    <<: *airflow-common
    ports:
      - "8080:8080"
    command: webserver
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler

  spark-master:
    image: bitnami/spark:3.5
    environment:
      SPARK_MODE: master
    ports:
      - "7077:7077"
      - "8090:8080"   # Spark UI (evita conflito com Airflow :8080)

  spark-worker:
    image: bitnami/spark:3.5
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 2G
      SPARK_WORKER_CORES: 2
    depends_on:
      - spark-master

  dashboard:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8501:8501"
    volumes:
      - ./data:/app/data
    env_file: .env
    environment:
      DEPLOY_ENV: spark
    depends_on:
      - ollama

  api:
    build:
      context: .
      dockerfile: Dockerfile.api
    ports:
      - "8000:8000"
    volumes:
      - ./data:/app/data
    env_file: .env
    environment:
      DEPLOY_ENV: spark

  ollama:
    image: ollama/ollama:latest
    ports:
      - "11434:11434"
    volumes:
      - ollama_models:/root/.ollama

volumes:
  ollama_models:
```

### 5.2 Subir o ambiente

```bash
# Primeira vez — inicializa o banco do Airflow
make airflow-init

# Sobe todos os serviços
make docker-up

# Baixa o modelo LLM no Ollama
make docker-pull-ollama

# Verificar que tudo está rodando
docker compose ps
```

### 5.3 Acessar as interfaces

| Serviço | URL | Credenciais |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Spark UI (master) | http://localhost:8090 | — |
| Dashboard Streamlit | http://localhost:8501 | — |
| API FastAPI (docs) | http://localhost:8000/docs | — |
| Ollama | http://localhost:11434 | — |

---

## Passo 6 — Pipeline Bronze com PySpark

> Objetivo: coletar dados brutos e persistir em Delta Lake Bronze com schema enforcement.

### 6.1 bronze_job.py

```python
# src/spark_jobs/bronze_job.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from src.connectors.camara_connector import CamaraConnector
from src.repositories.factory import get_repository
from datetime import date

def run():
    repo = get_repository()
    connector = CamaraConnector()

    # Schema explícito — garante consistência entre execuções
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("nome", StringType(), False),
        StructField("siglaPartido", StringType(), True),
        StructField("siglaUf", StringType(), True),
        StructField("urlFoto", StringType(), True),
        StructField("email", StringType(), True),
    ])

    df = connector.fetch_all()

    # Adiciona metadados de ingestão
    df = df.with_columns([
        pl.lit(str(date.today())).alias("_ingest_date"),
        pl.lit("camara_api").alias("_source"),
    ])

    # Escreve em Delta Bronze — modo overwrite para dados estáticos
    # O Delta mantém histórico mesmo com overwrite (time travel)
    repo.write(df, table="bronze/deputados", mode="overwrite")
    print(f"Bronze ingerido: {len(df)} deputados")

if __name__ == "__main__":
    run()
```

### 6.2 Executar o job manualmente

```bash
make spark-submit JOB=bronze_job
# ou diretamente:
uv run python -m src.spark_jobs.bronze_job
```

### 6.3 Verificar os dados no Delta

```python
# No notebook ou Python interativo
from src.repositories.factory import get_repository

repo = get_repository()

# Ler versão atual
df = repo.read("bronze/deputados")
print(df.shape)

# Ver histórico de operações
history = repo.history("bronze/deputados")
print(history.select(["version", "timestamp", "operation"]))

# Time travel — versão anterior
df_v0 = repo.time_travel("bronze/deputados", version=0)
```

---

## Passo 7 — Pipeline Silver com Delta Lake

> Objetivo: validar, normalizar e persistir dados com MERGE INTO (upsert ACID — sem duplicatas).

### 7.1 Schemas Pydantic para validação

```python
# src/spark_jobs/silver_job.py (trecho de validação)
from pydantic import BaseModel, field_validator
from datetime import date

class ParlamentarSchema(BaseModel):
    id: int
    nome: str
    sigla_partido: str
    sigla_uf: str
    url_foto: str | None = None
    email: str | None = None

    @field_validator("sigla_uf")
    @classmethod
    def uf_valida(cls, v):
        ufs = {"AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS",
               "MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC",
               "SP","SE","TO"}
        if v not in ufs:
            raise ValueError(f"UF inválida: {v}")
        return v
```

### 7.2 silver_job.py — MERGE INTO

```python
def run():
    repo = get_repository()

    # Lê Bronze
    df_raw = repo.read("bronze/deputados")

    # Valida com Pydantic (coleta erros sem parar o pipeline)
    valid_rows, errors = [], []
    for row in df_raw.to_dicts():
        try:
            validated = ParlamentarSchema(**row)
            valid_rows.append(validated.model_dump())
        except Exception as e:
            errors.append({"row": row, "error": str(e)})

    if errors:
        print(f"⚠️  {len(errors)} linhas inválidas — salvas em bronze/errors")
        repo.write(pl.DataFrame(errors), "bronze/errors", mode="append")

    df_silver = pl.DataFrame(valid_rows)

    # MERGE INTO — upsert ACID: atualiza existentes, insere novos, nunca duplica
    repo.merge_into(df_silver, table="silver/dim_parlamentar", on=["id"])
    print(f"Silver atualizado: {len(df_silver)} registros")
```

### 7.3 Executar e verificar

```bash
make spark-submit JOB=silver_job

# Verificar no Python
repo.read("silver/dim_parlamentar").head(5)
repo.history("silver/dim_parlamentar")
```

---

## Passo 8 — Pipeline Gold com PySpark

> Objetivo: agregar features para os modelos DS com Z-ORDER para performance.

### 8.1 gold_job.py

```python
# src/spark_jobs/gold_job.py
def run():
    repo = get_repository()
    spark = repo._spark  # SparkSession compartilhada

    # Registra as tabelas Silver como views temporárias para Spark SQL
    silver_parl = spark.read.format("delta").load("data/silver/dim_parlamentar")
    silver_eng  = spark.read.format("delta").load("data/silver/fato_engajamento")
    silver_leg  = spark.read.format("delta").load("data/silver/fato_legislativo")

    silver_parl.createOrReplaceTempView("parlamentar")
    silver_eng.createOrReplaceTempView("engajamento")
    silver_leg.createOrReplaceTempView("legislativo")

    # Agrega features para os modelos DS (Spark SQL)
    df_gold = spark.sql("""
        SELECT
            p.id                              AS parlamentar_id,
            p.nome,
            p.sigla_partido,
            p.sigla_uf,
            AVG(e.followers)                  AS avg_followers,
            AVG(e.engagement_rate)            AS avg_engagement_rate,
            MAX(e.followers)                  AS max_followers,
            COUNT(DISTINCT l.proposicao_id)   AS n_proposicoes,
            SUM(CASE WHEN l.voto='Sim' THEN 1 ELSE 0 END) AS n_votos_favoraveis,
            COUNT(DISTINCT l.proposicao_id)
                FILTER (WHERE l.voto='Sim')   AS n_aprovadas
        FROM parlamentar p
        LEFT JOIN engajamento e ON p.id = e.parlamentar_id
        LEFT JOIN legislativo  l ON p.id = l.parlamentar_id
        GROUP BY p.id, p.nome, p.sigla_partido, p.sigla_uf
    """)

    # Escreve Gold com Z-ORDER por parlamentar_id (acelera queries no dashboard)
    df_gold.write.format("delta").mode("overwrite").save("data/gold/features")
    repo.optimize("gold/features", z_order_by=["parlamentar_id"])

    print(f"Gold gerado: {df_gold.count()} deputados")
```

---

## Passo 9 — Orquestrar com Apache Airflow

> Objetivo: automatizar e monitorar todo o pipeline com DAGs, sensores e retries.

### 9.1 ingest_dag.py

```python
# dags/ingest_dag.py
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

@dag(
    dag_id="ingest_dag",
    schedule="0 6 * * *",          # Todo dia às 6h
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "email_on_failure": True,
    },
    tags=["bronze", "ingestão"],
)
def ingest_dag():

    ingest_camara = SparkSubmitOperator(
        task_id="ingest_camara",
        application="src/spark_jobs/bronze_job.py",
        conn_id="spark_default",
        packages="io.delta:delta-spark_2.12:3.1.0",
        conf={"spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"},
    )

    # Após Bronze estar pronto, dispara o pipeline de transformação
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="transform_dag",
    )

    ingest_camara >> trigger_transform

ingest_dag()
```

### 9.2 transform_dag.py

```python
# dags/transform_dag.py
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}
DELTA_PKG = "io.delta:delta-spark_2.12:3.1.0"

@dag(
    dag_id="transform_dag",
    schedule=None,      # Acionado pelo ingest_dag
    start_date=datetime(2024, 1, 1),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["silver", "gold", "transformação"],
)
def transform_dag():

    # Sensor garante que Bronze existe antes de transformar
    wait_bronze = FileSensor(
        task_id="wait_bronze",
        filepath="data/bronze/deputados/_delta_log",
        poke_interval=30,
        timeout=600,
    )

    silver_job = SparkSubmitOperator(
        task_id="silver_transform",
        application="src/spark_jobs/silver_job.py",
        conn_id="spark_default",
        packages=DELTA_PKG,
        conf=SPARK_CONF,
    )

    gold_job = SparkSubmitOperator(
        task_id="gold_features",
        application="src/spark_jobs/gold_job.py",
        conn_id="spark_default",
        packages=DELTA_PKG,
        conf=SPARK_CONF,
    )

    wait_bronze >> silver_job >> gold_job

transform_dag()
```

### 9.3 models_dag.py

```python
# dags/models_dag.py
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

@dag(
    dag_id="models_dag",
    schedule=None,      # Acionado pelo transform_dag
    start_date=datetime(2024, 1, 1),
    tags=["gold", "modelos", "ds"],
)
def models_dag():

    wait_gold = FileSensor(
        task_id="wait_gold",
        filepath="data/gold/features/_delta_log",
        poke_interval=30,
        timeout=900,
    )

    @task
    def run_linear_models():
        from src.models.linear_models import LinearModelsRunner
        from src.repositories.factory import get_repository
        repo = get_repository()
        df = repo.read("gold/features")
        result = LinearModelsRunner().run(df)
        repo.write(pl.DataFrame([result.metrics]), "gold/model_results/linear", "append")

    @task
    def run_timeseries():
        from src.models.time_series import TimeSeriesRunner
        from src.repositories.factory import get_repository
        repo = get_repository()
        df = repo.read("silver/fato_engajamento")
        TimeSeriesRunner().run(df)

    @task
    def run_clustering():
        from src.models.clustering import ClusteringRunner
        from src.repositories.factory import get_repository
        repo = get_repository()
        df = repo.read("gold/features")
        ClusteringRunner().run(df)

    @task
    def run_score():
        from src.models.score import ScoreRunner
        from src.repositories.factory import get_repository
        repo = get_repository()
        df = repo.read("gold/features")
        ScoreRunner().run(df)

    # Modelos rodam em paralelo após Gold estar pronto
    wait_gold >> [run_linear_models(), run_timeseries(),
                  run_clustering(), run_score()]

models_dag()
```

### 9.4 Ativar os DAGs

```bash
# Via UI: acesse http://localhost:8080 → ative ingest_dag, transform_dag, models_dag

# Via CLI:
docker compose exec airflow-scheduler airflow dags unpause ingest_dag
docker compose exec airflow-scheduler airflow dags unpause transform_dag
docker compose exec airflow-scheduler airflow dags unpause models_dag

# Disparar manualmente para testar:
docker compose exec airflow-scheduler airflow dags trigger ingest_dag
```

---

## Passo 10 — Ciência de Dados

> Os modelos DS são completamente independentes do Spark — recebem pandas DataFrame e retornam `ModelResult`. Nenhum deles importa PySpark diretamente.

### 10.1 Modelos lineares (OLS + GLM)

```python
# src/models/linear_models.py
import polars as pl
import statsmodels.api as sm
import pandas as pd
from src.interfaces.model_runner import IModelRunner, ModelResult

class LinearModelsRunner:
    """OLS e GLM Poisson para explicar engajamento via variáveis legislativas."""

    def run(self, df: pl.DataFrame) -> ModelResult:
        pdf = df.to_pandas()
        pdf = pd.get_dummies(pdf, columns=["sigla_partido", "sigla_uf"], drop_first=True)

        y = pdf["avg_engagement_rate"]
        X = sm.add_constant(pdf[[c for c in pdf.columns
                                  if c.startswith(("n_", "sigla_", "avg_followers"))]])

        # OLS
        ols = sm.OLS(y, X).fit()

        # GLM Poisson para contagens (não-negativas)
        glm_pois = sm.GLM(
            pdf["n_proposicoes"], X,
            family=sm.families.Poisson()
        ).fit()

        return ModelResult(
            name="linear_models",
            metrics={
                "ols_r2": ols.rsquared,
                "ols_adj_r2": ols.rsquared_adj,
                "glm_aic": glm_pois.aic,
                "glm_deviance": glm_pois.deviance,
            },
            artifacts={
                "ols_summary": ols.summary().as_text(),
                "ols_params": ols.params.to_dict(),
                "ols_pvalues": ols.pvalues.to_dict(),
            }
        )
```

### 10.2 Séries temporais (ARIMA + Prophet)

```bash
# Instalar dependências do grupo models
uv sync --group models

# Rodar notebook de exploração
uv run jupyter lab notebooks/03_series_temporais.ipynb
```

### 10.3 Rodar todos os modelos manualmente

```bash
# Disparar o DAG de modelos via Airflow
docker compose exec airflow-scheduler airflow dags trigger models_dag

# Ou rodar individualmente
uv run python -m src.models.linear_models
uv run python -m src.models.time_series
uv run python -m src.models.clustering
uv run python -m src.models.score
```

### 10.4 Explorar resultados com time travel

```python
# Comparar resultado dos modelos de hoje com a semana passada
repo = get_repository()

# Versão atual
current = repo.read("gold/model_results/linear")

# Versão da semana passada (veja a versão no histórico)
history = repo.history("gold/model_results/linear")
last_week_version = history.filter(
    pl.col("timestamp") < pl.lit("2024-01-15")
).select("version").max().item()

old = repo.time_travel("gold/model_results/linear", version=last_week_version)
print("Mudança no R²:", current["ols_r2"].mean() - old["ols_r2"].mean())
```

---

## Passo 11 — IA local (NLP + RAG)

### 11.1 Instalar dependências pesadas

```bash
uv sync --group nlp
# ~2GB de download — inclui torch CPU e modelos HuggingFace
```

### 11.2 Sentimento com BERTimbau

```python
# src/models/nlp_sentiment.py
from transformers import pipeline
import polars as pl
from src.interfaces.model_runner import IModelRunner, ModelResult

class NLPSentimentRunner:
    def __init__(self):
        # Modelo BERT treinado em português
        self._pipe = pipeline(
            "text-classification",
            model="neuralmind/bert-base-portuguese-cased",
            tokenizer="neuralmind/bert-base-portuguese-cased",
        )

    def run(self, df: pl.DataFrame) -> ModelResult:
        texts = df["texto_discurso"].to_list()
        results = self._pipe(texts, batch_size=16, truncation=True)
        scores = [r["score"] if r["label"] == "POSITIVE" else -r["score"]
                  for r in results]
        df_out = df.with_columns(pl.Series("sentimento_score", scores))
        return ModelResult(
            name="nlp_sentiment",
            metrics={"mean_sentiment": sum(scores) / len(scores)},
            artifacts={"df": df_out}
        )
```

### 11.3 RAG com ChromaDB + Ollama

```python
# src/models/rag_indexer.py
import chromadb
from sentence_transformers import SentenceTransformer
import polars as pl

class RAGIndexer:
    def __init__(self, persist_path: str = "data/gold/embeddings"):
        self._client = chromadb.PersistentClient(path=persist_path)
        self._model = SentenceTransformer("paraphrase-multilingual-mpnet-base-v2")
        self._collection = self._client.get_or_create_collection("discursos")

    def index(self, df: pl.DataFrame) -> None:
        """Indexa todos os discursos com metadados."""
        texts = df["sumario"].to_list()
        embeddings = self._model.encode(texts, batch_size=32, show_progress_bar=True)
        self._collection.upsert(
            ids=[str(i) for i in df["id"].to_list()],
            embeddings=embeddings.tolist(),
            documents=texts,
            metadatas=df.select(["parlamentar_id", "data", "keywords"]).to_dicts()
        )
        print(f"Indexados {len(texts)} discursos")

    def search(self, query: str, k: int = 5, parlamentar_id: int | None = None) -> list:
        """Busca semântica com filtro opcional por deputado."""
        embedding = self._model.encode([query])[0]
        where = {"parlamentar_id": parlamentar_id} if parlamentar_id else None
        return self._collection.query(
            query_embeddings=[embedding.tolist()],
            n_results=k,
            where=where
        )
```

### 11.4 Testar o chatbot localmente

```bash
# Verificar Ollama
curl http://localhost:11434/api/generate \
  -d '{"model":"llama3.2","prompt":"Olá, tudo bem?","stream":false}'

# Rodar o indexador
uv run python -m src.models.rag_indexer

# Testar RAG
uv run python -c "
from src.models.rag_indexer import RAGIndexer
r = RAGIndexer()
results = r.search('reforma tributária')
print(results['documents'])
"
```

---

## Passo 12 — Dashboard e API

### 12.1 Rodar o dashboard

```bash
# Sem Docker
make dashboard
# → http://localhost:8501

# Com Docker (já incluído no docker-compose.yml)
make docker-up
```

### 12.2 Rodar a API

```bash
# Sem Docker
make api
# → http://localhost:8000/docs

# Testar endpoints
curl "http://localhost:8000/deputies?limit=10"
curl "http://localhost:8000/scores/ranking?partido=PT"
curl -X POST "http://localhost:8000/chat" \
     -H "Content-Type: application/json" \
     -d '{"query": "O que o deputado Fulano disse sobre educação?"}'
```

### 12.3 Adicionar mangum para Lambda

```python
# src/api/main.py
from fastapi import FastAPI
from mangum import Mangum
from src.api import deputies, scores, timeseries, chat

app = FastAPI(title="Gêmeo Digital Legislativo API")
app.include_router(deputies.router)
app.include_router(scores.router)
app.include_router(timeseries.router)
app.include_router(chat.router)

# Handler para AWS Lambda — transparente para uvicorn local
handler = Mangum(app, lifespan="off")
```

---

## Passo 13 — Deploy na AWS

### 13.1 Pré-requisitos

```bash
aws configure                  # configure credenciais
aws --version                  # 2.x
sam --version                  # 1.x

# Verificar que as imagens Docker estão prontas
make docker-build
```

### 13.2 Criar repositórios ECR

```bash
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=us-east-1

aws ecr create-repository --repository-name gemeo-dashboard --region $AWS_REGION
aws ecr create-repository --repository-name gemeo-models   --region $AWS_REGION
aws ecr create-repository --repository-name gemeo-nlp      --region $AWS_REGION
```

### 13.3 Push das imagens

```bash
make docker-push
# Equivalente a:
# docker tag + docker push para cada imagem → ECR
```

### 13.4 Deploy completo

```bash
# Primeira vez (interativo)
sam build
sam deploy --guided
# Responda: stack name, região, bucket S3, URIs ECR, parâmetros

# Deploys seguintes
make deploy
```

### 13.5 Equivalências na AWS

| Local | AWS |
|---|---|
| Airflow (Docker) | MWAA (Managed Workflows for Apache Airflow) |
| Spark standalone | EMR Serverless |
| Delta em `data/` | Delta em S3 (mesmo formato, zero migração) |
| ChromaDB local | OpenSearch Serverless |
| Ollama local | Bedrock (Claude Haiku) |
| Streamlit Docker | ECS Fargate |
| FastAPI local | API Gateway + Lambda |

### 13.6 Serviços AWS criados pelo template.yaml

| Serviço | Uso |
|---|---|
| AWS Lambda | Ingestão leve (ZIP) + modelos (container image) |
| Amazon S3 | Bronze / Silver / Gold em Delta |
| Amazon EMR Serverless | Spark jobs sob demanda |
| MWAA | Airflow gerenciado |
| OpenSearch Serverless | Vector store RAG |
| ECS Fargate | Dashboard Streamlit |
| API Gateway | Endpoints REST públicos |
| AWS Bedrock | LLM cloud |
| SSM Parameter Store | Secrets |

---

## Docker — referência completa

### Arquivos Docker do projeto

| Arquivo | Propósito |
|---|---|
| `Dockerfile` | Dashboard Streamlit → ECS Fargate |
| `Dockerfile.api` | FastAPI → Lambda container |
| `infra/airflow/Dockerfile` | Airflow customizado com PySpark |
| `infra/lambdas/models/Dockerfile` | Lambda: prophet + statsmodels (>250MB) |
| `infra/lambdas/nlp/Dockerfile` | Lambda: torch + transformers (~2GB) |
| `docker-compose.yml` | Ambiente local completo |
| `.dockerignore` | Exclui data/, .env, notebooks/ |

### Dockerfile — dashboard Streamlit

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install --no-cache-dir ".[dashboard]"
COPY src/dashboard ./src/dashboard
COPY src/interfaces ./src/interfaces
COPY src/repositories ./src/repositories
COPY config ./config
EXPOSE 8501
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1
ENTRYPOINT ["streamlit", "run", "src/dashboard/dashboard.py", \
            "--server.port=8501", "--server.address=0.0.0.0"]
```

### Dockerfile — Lambda NLP (torch)

```dockerfile
FROM public.ecr.aws/lambda/python:3.12
WORKDIR ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir \
    torch==2.2.0+cpu \
    transformers>=4.40 \
    sentence-transformers>=3.0 \
    chromadb>=0.5 \
    langchain>=0.2 \
    -f https://download.pytorch.org/whl/torch_stable.html
COPY src/ ./src/
COPY config/ ./config/
CMD ["src.models.rag_indexer.handler"]
```

### .dockerignore

```dockerignore
data/
*.duckdb
*.parquet
.env
notebooks/
.venv/
__pycache__/
*.pyc
.pytest_cache/
.ruff_cache/
.git/
```

---

## Comandos de referência (Makefile)

```makefile
# ── Ambiente ─────────────────────────────────────────────────────────
airflow-init:
    docker compose run --rm airflow-init

docker-up:
    docker compose up -d

docker-down:
    docker compose down

docker-pull-ollama:
    docker compose exec ollama ollama pull llama3.2

# ── Pipeline ─────────────────────────────────────────────────────────
spark-submit:
    uv run python -m src.spark_jobs.$(JOB)

run-pipeline:
    uv run python -m src.spark_jobs.bronze_job
    uv run python -m src.spark_jobs.silver_job
    uv run python -m src.spark_jobs.gold_job

# ── Delta Lake ───────────────────────────────────────────────────────
delta-history:
    uv run python -c "from src.repositories.factory import get_repository; \
                      print(get_repository().history('$(TABLE)'))"

delta-restore:
    uv run python -c "from src.repositories.factory import get_repository; \
                      get_repository().time_travel('$(TABLE)', $(VERSION))"

delta-optimize:
    uv run python -c "from src.repositories.factory import get_repository; \
                      get_repository().optimize('$(TABLE)', ['parlamentar_id'])"

# ── Airflow ──────────────────────────────────────────────────────────
dag-trigger:
    docker compose exec airflow-scheduler airflow dags trigger $(DAG)

dag-list:
    docker compose exec airflow-scheduler airflow dags list

# ── Serviços ─────────────────────────────────────────────────────────
dashboard:
    uv run streamlit run src/dashboard/dashboard.py

api:
    uv run uvicorn src.api.main:app --reload --port 8000

# ── Docker build/push ────────────────────────────────────────────────
docker-build:
    docker build -t gemeo-dashboard .
    docker build -t gemeo-airflow  -f infra/airflow/Dockerfile .
    docker build -t gemeo-models   -f infra/lambdas/models/Dockerfile .
    docker build -t gemeo-nlp      -f infra/lambdas/nlp/Dockerfile .

docker-push:
    aws ecr get-login-password | docker login --username AWS --password-stdin \
        $$(aws sts get-caller-identity --query Account --output text).dkr.ecr.$(AWS_REGION).amazonaws.com
    docker tag gemeo-models:latest $(ECR_MODELS_URI):latest && docker push $(ECR_MODELS_URI):latest
    docker tag gemeo-nlp:latest    $(ECR_NLP_URI):latest    && docker push $(ECR_NLP_URI):latest

# ── Qualidade ────────────────────────────────────────────────────────
test:
    uv run pytest tests/ -v --cov=src

lint:
    uv run ruff check . && uv run ruff format .

# ── Deploy ───────────────────────────────────────────────────────────
deploy:
    make docker-push
    sam build && sam deploy
```

---

## Pacotes utilizados

### Dados e pipeline

| Pacote | Versão | Finalidade |
|---|---|---|
| `pyspark` | ≥3.5 | Processamento distribuído |
| `delta-spark` | ≥3.1 | ACID, time travel, upsert, schema evolution |
| `apache-airflow` | ≥2.9 | Orquestração de DAGs |
| `apache-airflow-providers-apache-spark` | ≥4.8 | SparkSubmitOperator |
| `polars` | ≥1.0 | Processamento leve (connectors, testes) |
| `pydantic` | ≥2.0 | Validação de schema |
| `pydantic-settings` | ≥2.0 | Leitura do .env |
| `httpx` | ≥0.27 | Requisições API Câmara |
| `apify-client` | ≥1.7 | Instagram via Apify |
| `pytrends` | ≥4.9 | Google Trends |
| `boto3` | ≥1.34 | SDK AWS |
| `python-dotenv` | ≥1.0 | Variáveis de ambiente |

### Ciência de Dados

| Pacote | Finalidade |
|---|---|
| `statsmodels` | OLS, GLM Poisson/Binomial Negativo, STL |
| `scikit-learn` | K-Means, DBSCAN, PCA, StandardScaler |
| `pmdarima` | auto_arima para séries temporais |
| `prophet` | Previsão com eventos externos |
| `scipy` | Testes estatísticos |

### IA local / NLP

| Pacote | Finalidade |
|---|---|
| `transformers` | BERTimbau (HuggingFace) |
| `sentence-transformers` | Embeddings multilingues |
| `chromadb` | Vector store local |
| `langchain` | Pipeline RAG |
| `langchain-ollama` | Integração Ollama |
| `torch` | Backend HuggingFace (CPU) |

### Dashboard e API

| Pacote | Finalidade |
|---|---|
| `streamlit` | Dashboard web |
| `plotly` | Gráficos interativos |
| `fastapi` | API REST |
| `uvicorn` | Servidor ASGI |
| `mangum` | Adapter FastAPI → Lambda |

### Desenvolvimento

| Pacote | Finalidade |
|---|---|
| `pytest` + `pytest-cov` | Testes e cobertura |
| `ruff` | Lint e format |
| `moto` | Mock AWS para testes |
| `jupyterlab` | Notebooks |
| `aws-sam-cli` | Deploy serverless |

---

## Variáveis de ambiente

```dotenv
# ── Geral ────────────────────────────────────────────────────
DEPLOY_ENV=spark          # "spark" | "local" | "aws"
                          # spark  → DeltaRepository (PySpark)
                          # local  → LocalParquetRepository (DuckDB, sem Spark)
                          # aws    → S3Repository (produção)

# ── APIs externas ────────────────────────────────────────────
APIFY_API_KEY=apify_api_xxxxxxxxxxxxxxxxxx
APIFY_ACTOR_ID=apify/instagram-profile-scraper

# ── Spark (local) ────────────────────────────────────────────
SPARK_MASTER=local[*]     # "local[*]" para usar todos os cores
                          # "spark://spark-master:7077" para cluster Docker
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# ── Delta Lake ───────────────────────────────────────────────
DELTA_BASE_PATH=data/     # caminho base das tabelas Delta

# ── LLM local ────────────────────────────────────────────────
OLLAMA_MODEL=llama3.2     # llama3.2 | mistral | gemma2
OLLAMA_BASE_URL=http://localhost:11434

# ── AWS (apenas quando DEPLOY_ENV=aws) ───────────────────────
AWS_REGION=us-east-1
S3_BUCKET=gemeo-digital-data
OPENSEARCH_ENDPOINT=https://...
ECR_MODELS_URI=123456789.dkr.ecr.us-east-1.amazonaws.com/gemeo-models
ECR_NLP_URI=123456789.dkr.ecr.us-east-1.amazonaws.com/gemeo-nlp

# ── Score de influência (devem somar 1.0) ────────────────────
SCORE_PESO_FOLLOWERS=0.30
SCORE_PESO_ENGAJAMENTO=0.30
SCORE_PESO_ATIVIDADE=0.25
SCORE_PESO_TRENDS=0.15
```

---

## Equivalências local ↔ AWS

| Conceito | Local (laboratório) | AWS (produção) | Nota |
|---|---|---|---|
| **Orquestração** | Airflow (Docker) | MWAA | Mesmos DAGs, zero mudança |
| **Processamento** | Spark standalone | EMR Serverless | Mesmo código PySpark |
| **Storage** | Delta em `data/` | Delta em S3 | Mesmo formato, zero migração |
| **Vector store** | ChromaDB | OpenSearch Serverless | Troca via IVectorStore |
| **LLM** | Ollama | Bedrock (Claude Haiku) | Troca via langchain |
| **Dashboard** | `streamlit run` | ECS Fargate | Mesmo Dockerfile |
| **API** | uvicorn | API Gateway + Lambda | Mangum faz o adapter |
| **Lambdas leves** | Scripts locais | Lambda ZIP | SAM empacota |
| **Lambdas pesadas** | Scripts locais | Lambda container | ECR + SAM |

---

## Roadmap

### Fase 1 — Fundação SOLID 📋
- [ ] Criar estrutura de pastas
- [ ] Implementar interfaces (IConnector, IRepository, IModelRunner, IVectorStore)
- [ ] Implementar InMemoryRepository + testes
- [ ] Implementar DeltaRepository
- [ ] Configurar RepositoryFactory
- [ ] Configurar pyproject.toml com grupos de dependências
- [ ] Makefile base

### Fase 2 — Pipeline Bronze/Silver/Gold 📋
- [ ] CamaraConnector + SocialConnector + TrendsConnector
- [ ] bronze_job.py com PySpark + Delta
- [ ] Schemas Pydantic para Silver
- [ ] silver_job.py com MERGE INTO (upsert ACID)
- [ ] gold_job.py com Spark SQL + Z-ORDER
- [ ] Testes com mocks e SparkSession local

### Fase 3 — Orquestração Airflow 📋
- [ ] Subir ambiente Docker completo
- [ ] ingest_dag.py com SparkSubmitOperator
- [ ] transform_dag.py com sensor Bronze
- [ ] models_dag.py com tasks paralelas
- [ ] Configurar Connection Spark no Airflow

### Fase 4 — Ciência de Dados 📋
- [ ] AED (eda.py) + notebook 01
- [ ] OLS + GLM Poisson (linear_models.py) + notebook 04
- [ ] K-Means + DBSCAN + PCA (clustering.py) + notebook 02
- [ ] ARIMA + Prophet (time_series.py) + notebook 03
- [ ] Score de influência (score.py)

### Fase 5 — IA local 📋
- [ ] BERTimbau sentimento (nlp_sentiment.py) + notebook 05
- [ ] ChromaDB indexação (rag_indexer.py)
- [ ] Pipeline RAG LangChain + Ollama

### Fase 6 — Dashboard + API 📋
- [ ] Streamlit: todas as 6 páginas
- [ ] FastAPI: todos os endpoints + mangum
- [ ] Testes de integração dashboard ↔ Delta

### Fase 7 — Docker completo 📋
- [ ] Dockerfile dashboard
- [ ] Dockerfile API
- [ ] Dockerfile Airflow customizado
- [ ] Dockerfiles Lambdas pesadas (models + nlp)
- [ ] docker-compose.yml com todos os serviços
- [ ] Testes do ambiente Docker completo

### Fase 8 — Deploy AWS 📋
- [ ] S3Repository + testes com moto
- [ ] IVectorStore com OpenSearch
- [ ] SAM template completo
- [ ] CI/CD GitHub Actions
- [ ] Primeiro deploy e smoke tests produção

---

*Baseado em [Analise-deputados](https://github.com/Vini0606/Analise-deputados) — evoluído para laboratório completo de Engenharia de Dados com PySpark, Delta Lake, Airflow, SOLID, Docker e AWS Serverless.*
