from logging import Logger

import boto3

from src.core.env.env import ApplicationEnv

logger = Logger(__name__)


def create_boto3_session():
    profile = ApplicationEnv.AWS_PROFILE()
    if not profile:
        return boto3.Session()
    else:
        return boto3.Session(profile_name=profile)


def get_kf_secrets_manager():
    session = create_boto3_session()
    return session.client(
        service_name="secretsmanager",
        region_name=ApplicationEnv.AWS_DEFAULT_REGION_NAME(),
    )
