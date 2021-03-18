#!/usr/bin/env bash
#
# Train model
#
# Run "bin/train.sh --help" to see available options.

docker-compose run --service-ports --rm app \
  ./bin/apps/run_web_server.sh --reload
