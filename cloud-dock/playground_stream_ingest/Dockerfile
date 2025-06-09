# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry

RUN poetry config virtualenvs.create false

# Copy application code
COPY /playground_stream_ingest ./playground_stream_ingest

# Install Python dependencies
RUN poetry install --directory ./playground_stream_ingest

# Run the application
CMD gunicorn --bind :$PORT --workers 2 --timeout 120 --max-requests 1000 --max-requests-jitter 100 --graceful-timeout 30 --access-logfile - playground_stream_ingest.src.main:app
