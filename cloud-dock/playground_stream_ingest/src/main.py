from playground_stream_ingest.src.app import app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)  # nosec
