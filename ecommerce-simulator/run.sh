#!/bin/bash

# E-commerce Simulator Startup Script

# Set default environment variables
export STREAM_ENDPOINT="${STREAM_ENDPOINT:-https://playground-stream-ingest-1234567890-uc.a.run.app/api/transactions}"
export BATCH_BUCKET="${BATCH_BUCKET:-muz-designed-msc-data-ai-2025-playground_project}"
export SECRET_ID="${SECRET_ID:-playground_project_stream_secret}"
export GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT:-muz-designed-msc-data-ai-2025}"

echo "🛒 Starting E-commerce Data Simulator..."
echo "📊 Stream Endpoint: $STREAM_ENDPOINT"
echo "📦 Batch Bucket: $BATCH_BUCKET"
echo "🔐 Secret ID: $SECRET_ID"
echo "🌐 Project: $GOOGLE_CLOUD_PROJECT"
echo ""
echo "🚀 Starting Flask app on http://localhost:8000"
echo ""

# Install dependencies if needed
if [ ! -d "venv" ]; then
    echo "📦 Installing dependencies..."
    pip install -r requirements.txt
    echo ""
fi

# Start the Flask app
python app.py
