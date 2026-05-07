# config/settings.py
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Configurações centralizadas do projeto utilizando Pydantic.
    Lê automaticamente variáveis de ambiente e arquivos .env.
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # ── Geral ────────────────────────────────────────────────────
    DEPLOY_ENV: str = Field(default="spark")

    # ── APIs externas ────────────────────────────────────────────
    APIFY_API_KEY: str | None = Field(default=None)
    APIFY_ACTOR_ID: str = Field(default="apify/instagram-profile-scraper")

    # ── Spark (local) ────────────────────────────────────────────
    SPARK_MASTER: str = Field(default="local[*]")
    JAVA_HOME: str | None = Field(default=None)

    # ── Delta Lake ───────────────────────────────────────────────
    DELTA_BASE_PATH: str = Field(default="data/")

    # ── LLM local ────────────────────────────────────────────────
    OLLAMA_MODEL: str = Field(default="llama3.2")
    OLLAMA_BASE_URL: str = Field(default="http://localhost:11434")

    # ── AWS (apenas quando DEPLOY_ENV=aws) ───────────────────────
    AWS_REGION: str = Field(default="us-east-1")
    S3_BUCKET: str | None = Field(default=None)
    OPENSEARCH_ENDPOINT: str | None = Field(default=None)
    ECR_MODELS_URI: str | None = Field(default=None)
    ECR_NLP_URI: str | None = Field(default=None)

    # ── Score de influência (devem somar 1.0) ────────────────────
    SCORE_PESO_FOLLOWERS: float = Field(default=0.30)
    SCORE_PESO_ENGAJAMENTO: float = Field(default=0.30)
    SCORE_PESO_ATIVIDADE: float = Field(default=0.25)
    SCORE_PESO_TRENDS: float = Field(default=0.15)


# Instância única para ser importada em todo o projeto
settings = Settings()
