from __future__ import annotations
from typing import Any

from pydantic import BaseModel


class MongoMetricsClient(BaseModel):
    """Client used to insert metrics to the target collection."""

    # TODO: Find a better way to type mongo_client
    mongo_client: Any
    db_name: str
    collection_name: str
