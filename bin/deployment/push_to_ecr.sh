#!/usr/bin/env bash
#
# Push docker image directly

echo "$IS_MAIN_BRANCH"

if [[ "$IS_MAIN_BRANCH" == "main" ]]; then
  echo "$AWS_FULL_IMAGE_NAME"
  echo "$REPOSITORY_URI":latest
  docker push "$REPOSITORY_URI":latest
  docker push "$AWS_FULL_IMAGE_NAME"
else
  echo "Will not push docker build"
fi
