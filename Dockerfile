# -- Base image --------------------------------------------------------------
FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends         build-essential libpq-dev curl &&     rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3     CMD curl -f http://localhost:8000/health || exit 1

# Default: run analytics then start server
CMD ["python", "run_all.py"]
