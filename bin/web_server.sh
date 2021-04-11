#!/usr/bin/env bash
#
# Train model
#
# Run "bin/train.sh --help" to see available options.

docker-compose run --service-ports --rm app \
  uvicorn src.web.app.factory:create_app --factory --host 0.0.0.0 --port 8080 --reload
