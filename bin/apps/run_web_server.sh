#!/usr/bin/env bash
#
# Start the webserver

# shellcheck disable=SC2086
gunicorn -b 0.0.0.0:8080 -w 2 --threads 2 --timeout 0 -k uvicorn.workers.UvicornWorker src.web.app.factory:create_app
