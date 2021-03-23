FROM 533333767769.dkr.ecr.us-gov-west-1.amazonaws.com/pytorch:1.6.0-cuda10.1-cudnn7-devel
#FROM 533333767769.dkr.ecr.us-gov-west-1.amazonaws.com/python:3.8-slim

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
RUN pip install --upgrade pip && pip install --ignore-installed --no-cache-dir -r requirements.txt

# Configure spaCy
RUN python -m spacy download en_core_web_sm

# Download and install gretel-tools + sync the fasttext model
RUN git clone https://github.com/gretelai/gretel-tools.git
RUN (cd gretel-tools && pip install -U -I .)
RUN python -c 'from gretel_tools import headers; h = headers.HeaderAnalyzer()'

# Switch to non-root user
RUN useradd -m appuser && chown -R appuser /workspace
USER appuser

# Copy project files
COPY . .

RUN python -m src.install

# This should stay the entrypoint. Issue commands for new behavior.
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
