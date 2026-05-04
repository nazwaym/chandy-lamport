FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install psycopg2-binary --break-system-packages 2>/dev/null || \
    pip install psycopg2-binary

# Copy source code
COPY worker.py /app/worker.py

# Buat direktori checkpoint
RUN mkdir -p /checkpoint_storage

CMD ["python", "-u", "/app/worker.py"]
