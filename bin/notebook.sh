#!/usr/bin/env bash
#
# Run Jupyter Notebook server on http://localhost:8888/.

docker-compose run --rm -p 8888:8888 \
  --entrypoint jupyter app notebook \
  --ip=0.0.0.0 \
  --port=8888 \
  --notebook-dir=notebooks \
  --allow-root \
  --NotebookApp.token='' \
  --NotebookApp.password=''
