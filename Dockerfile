FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Install supercronic for cron scheduling in Docker
ADD https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-amd64 /usr/local/bin/supercronic
RUN chmod +x /usr/local/bin/supercronic

# Copy project files and source code
COPY pyproject.toml uv.lock README.md ./
COPY marketdata/ marketdata/

# Install dependencies + project package
RUN uv sync --frozen --no-dev

# Copy configs and gateway scripts
COPY configs/ configs/
COPY gateway/ gateway/

# Patch config for Docker: services connect via Docker network, not localhost
RUN sed -i 's/ib_host:.*/ib_host: "ib-gateway"/' configs/default.yaml \
    && sed -i 's/\r$//' gateway/crontab
