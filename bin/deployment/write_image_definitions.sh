#!/usr/bin/env bash
#
# This writes our image definitions file for CodeDeploy to deploy our Frontend API service

printf '[{"name":"data_compliance_service","imageUri":"%s"}]' $AWS_FULL_IMAGE_NAME > imagedefinitions.json
