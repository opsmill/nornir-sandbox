# Image of the lab's Ray nodes (head and site workers), built from this repository.
#
# Every Ray node must run the same Python minor version and the same Ray version as the
# driver, so the image installs the project from uv.lock with the driver's Python.
# Build context: the repository root (see compose.yml).
FROM python:3.12.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.1 /uv /usr/local/bin/uv

# Ray must not start workers with `uv run` (see src/nornir_ray/__init__.py).
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    RAY_ENABLE_UV_RUN_RUNTIME_ENV=0

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev
ENV PATH=/app/.venv/bin:$PATH
