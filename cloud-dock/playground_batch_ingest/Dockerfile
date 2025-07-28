# Use Python 3.11 slim image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry

RUN poetry config virtualenvs.create false

# Copy application code
COPY /playground_batch_ingest ./playground_batch_ingest

# Install Python dependencies
RUN poetry install --directory ./playground_batch_ingest

# Create temp directories for file downloads and keys
RUN mkdir -p /tmp/batch_files /tmp/keys

# Run the application
CMD gunicorn --bind :$PORT --workers 2 --timeout 120 --max-requests 1000 --max-requests-jitter 100 --graceful-timeout 30 --access-logfile - playground_batch_ingest.src.main:app
