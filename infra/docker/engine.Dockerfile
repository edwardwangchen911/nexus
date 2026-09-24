ARG NEXUS_ENGINE_BASE_IMAGE=python:3.12-slim@sha256:2c941e860699f878900b0edc2403613c234d4b32eda3cc9fa7036991a2a63c4a
FROM ${NEXUS_ENGINE_BASE_IMAGE}

WORKDIR /app
COPY infra/docker/uv-requirements.txt /tmp/uv-requirements.txt
RUN python -m pip install --no-cache-dir --require-hashes -r /tmp/uv-requirements.txt \
    && rm /tmp/uv-requirements.txt
COPY pyproject.toml uv.lock ./
COPY engine/pyproject.toml engine/pyproject.toml
COPY integrations/fosun_gateway/pyproject.toml integrations/fosun_gateway/pyproject.toml
COPY contracts/strategy contracts/strategy
COPY contracts/agent contracts/agent
COPY engine/src engine/src
COPY engine/alembic.ini engine/alembic.ini
COPY engine/alembic engine/alembic
RUN uv sync --frozen --no-dev --package nexus-engine \
    && groupadd --system nexus \
    && useradd --system --gid nexus --home-dir /app --shell /usr/sbin/nologin nexus \
    && chown --recursive nexus:nexus /app

ENV PATH="/app/.venv/bin:$PATH"
USER nexus
CMD ["nexus-engine-api"]
