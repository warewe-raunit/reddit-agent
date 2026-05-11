# syntax=docker/dockerfile:1.6

# Reddit Opportunity Fetcher API
# Read-only FastAPI service. Does NOT require Playwright browsers at runtime
# (the opportunity pipeline talks to Reddit via HTTP only).
# Set BUILD_PLAYWRIGHT_BROWSERS=1 if you later need them inside this image.

FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      tini \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip \
 && pip install -r requirements.txt

ARG BUILD_PLAYWRIGHT_BROWSERS=0
RUN if [ "$BUILD_PLAYWRIGHT_BROWSERS" = "1" ]; then \
      playwright install --with-deps chromium ; \
    fi

COPY . .

RUN useradd --create-home --uid 1001 appuser \
 && mkdir -p /app/sessions /app/logs \
 && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8000/health || exit 1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips=*"]
