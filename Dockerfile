FROM python:3.11-slim

# Install system dependencies required for building C extensions (tgcrypto) and health checks (curl)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Create directories for persistent data and media library
RUN mkdir -p /app/data /media

EXPOSE 7000

CMD ["python", "main.py"]
