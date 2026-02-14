FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml .
COPY .env .

RUN pip install --no-cache-dir .[dev]

COPY . /app/async_consumer/

RUN useradd --create-home --shell /bin/bash appuser && chown -R appuser:appuser /app
USER appuser
ENV PYTHONPATH=/app