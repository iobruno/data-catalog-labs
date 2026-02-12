# Builder stage
FROM ghcr.io/astral-sh/uv:python3.11-trixie-slim AS builder

WORKDIR /datahub

COPY uv.lock pyproject.toml README.md ./

RUN --mount=type=cache,target=/root/.cache/uv \
    UV_COMPILE_BYTECODE=1 \
    uv pip install --system --no-cache .

ADD datahub_airbyte     /datahub/datahub_airbyte
ADD recipe.yml          /datahub/recipe.yml

# Runner stage
FROM python:3.11-slim AS runner

WORKDIR /datahub/

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /datahub /datahub/

ENV AIRBYTE_SERVER_URL=
ENV AIRBYTE_CLIENT_ID=
ENV AIRBYTE_CLIENT_SECRET=
ENV AIRBYTE_CONNECTION_ID=
ENV AIRFLOW_DAG_NAME=
ENV AIRFLOW_TASK_NAME=

ENTRYPOINT ["sh", "-c", "datahub ingest -c recipe.yml"]