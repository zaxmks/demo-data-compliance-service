#!/usr/bin/env bash
#
# Start the webserver

# shellcheck disable=SC2086
uvicorn src.web.app.factory:create_app --factory --host 0.0.0.0 --port 8081 $1
