#!/usr/bin/env bash
#
# The task definition replacement and push

echo "$IS_MAIN_BRANCH"

if [[ "$IS_MAIN_BRANCH" == "main" ]]; then
  # shellcheck disable=SC2002
  cat taskdef.json.template | envsubst > taskdef.json
  aws ecs register-task-definition --cli-input-json file://./taskdef.json
else
  echo "Will not push publish task definition"
fi
