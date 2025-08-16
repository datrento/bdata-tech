FROM python:3.10-slim

WORKDIR /kafka

# System deps (optional but helpful for SSL/certs)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy producer requirements and install
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

# Default command can be overridden by compose `command:`
CMD ["python", "producers/market_data_producer.py"]


