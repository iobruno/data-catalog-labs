# Builder stage
FROM ghcr.io/astral-sh/uv:python3.11-trixie-slim AS builder

WORKDIR /datahub

COPY uv.lock pyproject.toml README.md ./
COPY recipes /datahub/recipes
COPY connectors /datahub/connectors

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_COMPILE_BYTECODE=1 \
    uv pip install --system --no-cache .

# Builder stage
FROM python:3.11-slim AS runner

WORKDIR /datahub/

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /datahub /datahub/

ENV GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp_credentials.json

ENTRYPOINT ["sh", "-c", "datahub ingest -c recipes/bigquery.yml"]
