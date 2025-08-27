FROM python:3.10-slim

WORKDIR /price_elasticity

# Optional: install minimal OS deps for wheels if needed
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip setuptools wheel \
    && pip install -r requirements.txt

# Copy updater code
COPY . .

ENV PYTHONUNBUFFERED=1


