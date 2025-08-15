# Minimal FastAPI Container - Optimized for Speed
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install only curl for health checks (minimal system deps)
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy and install Python deps first (better caching)
COPY apis/requirements.txt .
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt

# Copy source code
COPY apis/ .

# Expose port
EXPOSE 8000

# Use uvicorn directly for better performance
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
