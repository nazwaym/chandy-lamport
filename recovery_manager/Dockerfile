FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install psycopg2-binary python-dateutil --break-system-packages 2>/dev/null || \
    pip install psycopg2-binary python-dateutil

# Copy source code
COPY recovery_manager.py /app/recovery_manager.py

CMD ["python", "-u", "/app/recovery_manager.py"]
