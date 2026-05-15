# syntax=docker/dockerfile:1
FROM python:3.13-slim

# Pull uv from its official image — fastest Python package manager
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev --no-install-project

COPY src/ ./src/

# OAuth is the only supported auth mode in a container (no OS keyring available)
ENV USE_OAUTH=true

EXPOSE 9090

CMD ["uv", "run", "--no-sync", "-m", "src.server"]
