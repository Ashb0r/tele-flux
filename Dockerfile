FROM python:3.11-slim

# Install system dependencies required for building C extensions (tgcrypto)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Create directory for persistent data
RUN mkdir -p /app/data

EXPOSE 7000

CMD ["python", "main.py"]
