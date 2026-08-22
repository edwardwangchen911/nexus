FROM ghcr.io/astral-sh/uv:0.8.22 AS uv

FROM python:3.12-slim

COPY --from=uv /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY engine/pyproject.toml engine/pyproject.toml
COPY integrations/fosun_gateway/pyproject.toml integrations/fosun_gateway/pyproject.toml
COPY integrations/fosun_gateway/src integrations/fosun_gateway/src
COPY contracts/providers/fosun/manifest.json contracts/providers/fosun/manifest.json
RUN uv sync --frozen --no-dev --package nexus-fosun-gateway \
    && groupadd --system nexus \
    && useradd --system --gid nexus --home-dir /app --shell /usr/sbin/nologin nexus \
    && chown --recursive nexus:nexus /app

ENV PATH="/app/.venv/bin:$PATH"
USER nexus
CMD ["nexus-fosun-gateway"]
