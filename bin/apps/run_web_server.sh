#!/usr/bin/env bash
#
# Start the webserver
uvicorn src.web.app.factory:create_app --factory --host 0.0.0.0 --port 80
