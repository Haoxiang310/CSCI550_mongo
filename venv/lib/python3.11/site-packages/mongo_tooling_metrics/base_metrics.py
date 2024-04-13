from __future__ import annotations
import atexit
import logging
import sys
from pydantic import BaseModel
import pymongo
from mongo_tooling_metrics import register_hook
from mongo_tooling_metrics.client import MongoMetricsClient

from mongo_tooling_metrics.errors import InvalidMetricsSetup
from mongo_tooling_metrics.lib.hooks import ExitHook

logger = logging.getLogger('tooling_metrics')
METRICS_FAILURE_LOG = "\nMetrics Collection Failed -- execution of this program should not be affected.\n"


class MongoMetrics(BaseModel):
    """Base class for a top-level metrics objects."""

    @classmethod
    def generate_metrics(cls, *args, **kwargs) -> MongoMetrics:
        """Class method to generate this metrics object -- will be executed before process exit."""
        raise InvalidMetricsSetup(
            "'generate_metrics' will be used to construct the top-level metrics object and must be defined."
        )

    @staticmethod
    def get_mongo_metrics_client() -> MongoMetricsClient:
        """Class method to get a mongo metrics client for these metrics."""
        raise InvalidMetricsSetup(
            "'get_mongo_metrics_client' must be defined so that metrics can be inserted to your cluster."
        )

    @staticmethod
    def should_collect_metrics() -> bool:
        """Determine whether metrics collection should even be registered or not."""
        # Default to always collecting metrics -- this can be overwritten.
        return True

    @staticmethod
    def initialize_hooks() -> None:
        """Initialize any hooks that these metrics rely on here -- this will get called after the metrics are registered."""
        # Default to registering the ExitHook -- this can be overwritten.
        sys.exit = register_hook(ExitHook(original_fn=sys.exit))

    def is_malformed(self) -> bool:
        """Determine whether these metrics are malformed (have all expected fields/data)."""
        # Default to metrics not being malformed -- this can be overwritten.
        return False

    @classmethod
    def register_metrics(
        cls,
        **kwargs,
    ) -> None:
        """Register the metrics to be generated and persisted at process exit -- kwargs will be passed to 'generate_metrics'."""
        if not cls.should_collect_metrics():
            return
        cls.initialize_hooks()
        atexit.register(
            cls._safe_save_metrics,
            **kwargs,
        )

    @classmethod
    def _safe_save_metrics(cls, **kwargs) -> None:
        """Save metrics and gracefully catch & log exceptions."""
        try:
            mongo_metrics_client = cls.get_mongo_metrics_client()
            metrics_dict = cls.generate_metrics(**kwargs).dict()
            with pymongo.timeout(1):
                mongo_metrics_client.mongo_client[mongo_metrics_client.db_name][
                    mongo_metrics_client.collection_name].insert_one(metrics_dict)
        except Exception as _:
            logger.warning(METRICS_FAILURE_LOG)
