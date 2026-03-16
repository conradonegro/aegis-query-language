FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Layer 1: install dependencies (cached independently of source code)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --compile-bytecode

# Layer 2: source code
COPY . .

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
