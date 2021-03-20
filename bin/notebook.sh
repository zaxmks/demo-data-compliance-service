#!/usr/bin/env bash
#
# Run Jupyter Notebook server on http://localhost:8888/.  was 88

docker-compose run --rm -p 8889:8889 \
  --entrypoint jupyter app notebook \
  --ip=0.0.0.0 \
  --port=8889 \
  --notebook-dir=notebooks \
  --allow-root \
  --NotebookApp.token='' \
  --NotebookApp.password=''
