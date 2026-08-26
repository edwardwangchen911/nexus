FROM ghcr.io/astral-sh/uv:0.8.22 AS uv

FROM python:3.12-slim

COPY --from=uv /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY engine/pyproject.toml engine/pyproject.toml
COPY integrations/fosun_gateway/pyproject.toml integrations/fosun_gateway/pyproject.toml
COPY contracts/strategy contracts/strategy
COPY engine/src engine/src
COPY engine/alembic.ini engine/alembic.ini
COPY engine/alembic engine/alembic
RUN uv sync --frozen --no-dev --package nexus-engine

ENV PATH="/app/.venv/bin:$PATH"
CMD ["nexus-engine-api"]
