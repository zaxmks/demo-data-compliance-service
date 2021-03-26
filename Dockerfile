FROM 533333767769.dkr.ecr.us-gov-west-1.amazonaws.com/python:3.8-slim

WORKDIR /workspace

# Flushes stdout immediately, instead of waiting. Logging is easy to read.
ENV PYTHONUNBUFFERED=1

# Update base environment
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y \
    build-essential \
    git \
    gcc \
    # https://github.com/Yelp/dumb-init#why-you-need-an-init-system
    dumb-init \
    && rm -rf /var/lib/apt/lists/*

# Install pip packages
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --ignore-installed --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

RUN python -m src.install

RUN python -m src.validate_pytorch

# This should stay the entrypoint. Issue commands for new behavior.
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
