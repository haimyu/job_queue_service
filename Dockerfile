FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.13.7 /uv /bin/uv

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# 1) Third-party dependencies only: cached until pyproject/uv.lock change
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# 2) Your package: src layout must be installed to be importable
COPY README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

RUN useradd --create-home appuser
USER appuser

# Exec form: python is PID 1 and receives SIGTERM directly
CMD ["uvicorn", "job_queue_service.api:app", "--host", "0.0.0.0", "--port", "8000"]