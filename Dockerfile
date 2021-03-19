#FROM 533333767769.dkr.ecr.us-gov-west-1.amazonaws.com/pytorch:1.6.0-cuda10.1-cudnn7-devel
FROM 533333767769.dkr.ecr.us-gov-west-1.amazonaws.com/python:3.8-slim

WORKDIR /workspace

# Flushes stdout immediately, instead of waiting. Logging is easy to read.
ENV PYTHONUNBUFFERED=1

# Update base environment
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y \
    htop \
    vim \
    git \
    wget \
    lsb-release \
    libpq-dev \
    # https://github.com/Yelp/dumb-init#why-you-need-an-init-system
    dumb-init \
    && rm -rf /var/lib/apt/lists/*

# Install pip packages
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && python -m src.install

# Copy project files
COPY . .

# This should stay the entrypoint. Issue commands for new behavior.
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
