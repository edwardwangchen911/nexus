FROM ghcr.io/astral-sh/uv:0.12.1@sha256:cf4eedcaa81655197f625739489effcbe71b61ceb1506f332c3facae5deceded AS uv

FROM python:3.12-slim@sha256:2c941e860699f878900b0edc2403613c234d4b32eda3cc9fa7036991a2a63c4a

COPY --from=uv /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY engine/pyproject.toml engine/pyproject.toml
COPY integrations/fosun_gateway/pyproject.toml integrations/fosun_gateway/pyproject.toml
COPY integrations/fosun_gateway/src integrations/fosun_gateway/src
COPY contracts/providers/fosun/manifest.json contracts/providers/fosun/manifest.json
RUN apt-get update \
    && apt-get install --yes --no-install-recommends git \
    && uv sync --frozen --no-dev --no-editable --package nexus-fosun-gateway \
    && apt-get purge --yes --auto-remove git \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system nexus \
    && useradd --system --gid nexus --home-dir /app --shell /usr/sbin/nologin nexus \
    && chown --recursive nexus:nexus /app \
    && mkdir -p /var/lib/nexus-fosun-gateway \
    && chown nexus:nexus /var/lib/nexus-fosun-gateway \
    && chmod 0700 /var/lib/nexus-fosun-gateway \
    && mkdir -p /run/nexus/fosun-activation /run/nexus/fosun-control \
    && chown root:root /run/nexus/fosun-activation /run/nexus/fosun-control \
    && chmod 0755 /run/nexus/fosun-activation /run/nexus/fosun-control

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8081 8082
USER nexus
ENTRYPOINT []
CMD ["nexus-fosun-gateway"]
