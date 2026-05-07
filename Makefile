.PHONY: run test lint dashboard api docker-up deploy

# ── Pipeline de Dados ────────────────────────────────────────────────
run:
	uv run python -m src.spark_jobs.bronze_job
	uv run python -m src.spark_jobs.silver_job
	uv run python -m src.spark_jobs.gold_job

# ── Qualidade e Testes ───────────────────────────────────────────────
test:
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format .

# ── Serviços Locais ──────────────────────────────────────────────────
dashboard:
	uv run streamlit run src/dashboard/dashboard.py

api:
	uv run uvicorn src.api.main:app --reload --port 8000

# ── Infraestrutura ───────────────────────────────────────────────────
docker-up:
	docker compose up -d

deploy:
	make docker-push
	sam build && sam deploy