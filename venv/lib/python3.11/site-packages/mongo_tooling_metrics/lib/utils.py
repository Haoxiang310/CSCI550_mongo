from __future__ import annotations
import os
from pydantic import BaseModel

import boto3
import pymongo
import urllib

from mongo_tooling_metrics.client import MongoMetricsClient

MONGOD_INTENRAL_DISTRO_FILEPATH = '/etc/mongodb-distro-name'
SUPPORTED_VIRTUAL_WORKSTATION_NAMES = set(
    ["ubuntu2204-workstation-graviton", "ubuntu2204-workstation", "ubuntu1804-workstation"])


def _is_virtual_workstation() -> bool:
    """Detect whether this is a MongoDB internal virtual workstation."""
    try:
        with open(MONGOD_INTENRAL_DISTRO_FILEPATH, 'r') as file:
            return file.read().strip() in SUPPORTED_VIRTUAL_WORKSTATION_NAMES
    except Exception as _:
        return False


TOOLING_METRICS_OPT_OUT = "TOOLING_METRICS_OPT_OUT"


def _has_metrics_opt_out() -> bool:
    """Check whether the opt out environment variable is set."""
    return os.environ.get(TOOLING_METRICS_OPT_OUT, None) == '1'


def should_collect_internal_metrics() -> bool:
    """Check whether we should collect internal metrics for this host."""
    return _is_virtual_workstation() and not _has_metrics_opt_out()


# This is a MongoDB internal cluster used to collect internal tooling metrics and usage.
# This cluster is only accessible from MongoDB employee virtual workstations.
# Tooling metrics are only collected for MongoDB employees.

INTERNAL_TOOLING_METRICS_HOSTNAME = "mongodb+srv://dev-metrics-pl-0.kewhj.mongodb.net"


class _MongoMetricsCreds(BaseModel):
    """Object for storing mongo aws creds after assuming an aws role."""
    id: str
    key: str
    token: str

    @classmethod
    def assume_aws_passwordless_auth_role(cls) -> _MongoMetricsCreds:
        """Assume the altas_passwordless_role_production role and return creds."""
        response = boto3.client('sts').assume_role(
            RoleArn='arn:aws:iam::557821124784:role/evergreen/altas_passwordless_role_production',
            RoleSessionName='mongo-tooling-metrics',
        )
        return cls(
            id=response['Credentials']['AccessKeyId'],
            key=response['Credentials']['SecretAccessKey'],
            token=urllib.parse.quote_plus(response['Credentials']['SessionToken']),
        )


def get_internal_mongo_metrics_client() -> MongoMetricsClient:
    """Construct client needed to setup metrics collection."""
    creds = _MongoMetricsCreds.assume_aws_passwordless_auth_role()
    return MongoMetricsClient(
        mongo_client=pymongo.MongoClient(
            host=INTERNAL_TOOLING_METRICS_HOSTNAME,
            username=creds.id,
            password=creds.key,
            authMechanism='MONGODB-AWS',
            authSource='$external',
            authMechanismProperties=f"AWS_SESSION_TOKEN:{creds.token}",
            socketTimeoutMS=1000,
            serverSelectionTimeoutMS=1000,
            connectTimeoutMS=1000,
            waitQueueTimeoutMS=1000,
            retryWrites=False,
            connect=False,
        ),
        db_name='metrics',
        collection_name='tooling_metrics',
    )
