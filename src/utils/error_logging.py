from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

import pymongo
from bson.objectid import ObjectId

MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
MONGO_DATABASE_NAME = "etl_pipeline_logs"
MONGO_COLLECTION_NAME = "process_tree"


class ErrorSeverity(Enum):
    INFO = 0
    WARNING = 1
    ERROR = 2
    CRITICAL = 3


@dataclass
class Error:
    severity: ErrorSeverity
    function: str
    description: str
    timestamp: datetime = field(default_factory=datetime.now)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            self.severity.name: {
                "function": self.function,
                "description": self.description,
                "timestamp": self.timestamp,
                "details": self.details,
            }
        }


class DatabaseConnection:
    def __init__(
        self,
        connection_string: str = MONGO_CONNECTION_STRING,
        database_name: str = MONGO_DATABASE_NAME,
        collection_name: str = MONGO_COLLECTION_NAME,
    ):
        self._client: pymongo.MongoClient = pymongo.MongoClient(connection_string)
        self._db = self._client[database_name]
        self._collection = self._db[collection_name]

    @property
    def collection(self) -> pymongo.collection.Collection:
        return self._collection


class ErrorLogger:
    def __init__(self, connection: DatabaseConnection):
        self._connection = connection

    def log_error(self, process_id: ObjectId, error: Error) -> None:
        self._connection.collection.update_one(
            {"_id": process_id}, {"$push": {"errors": error.to_dict()}}
        )

    def create_process(self, name: str, parent_id: Optional[ObjectId] = None) -> Optional[ObjectId]:
        process = {
            "name": name,
            "start_time": datetime.now(),
            "end_time": None,
            "errors": [],
            "children": [],
        }
        if parent_id:
            result = self._connection.collection.update_one(
                {"_id": parent_id}, {"$push": {"children": process}}
            )
            if result.modified_count == 1:
                return self._get_child_id(parent_id, name)
        return self._connection.collection.insert_one(process).inserted_id

    def end_process(self, process_id: ObjectId) -> None:
        self._connection.collection.update_one(
            {"_id": process_id}, {"$set": {"end_time": datetime.now()}}
        )

    def _get_child_id(self, parent_id: ObjectId, child_name: str) -> Optional[ObjectId]:
        parent_process = self._connection.collection.find_one(
            {"_id": parent_id, "children.name": child_name}, {"children.$": 1}
        )
        if parent_process and parent_process.get("children"):
            return parent_process["children"][0].get("_id")
        return None


class ProcessContext:
    def __init__(self, name: str, error_logger: ErrorLogger, parent_id: ObjectId):
        self.name = name
        self._error_logger = error_logger
        self.process_id = self._error_logger.create_process(name, parent_id)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._error_logger.end_process(self.process_id)

    def log_error(
        self, severity: ErrorSeverity, description: str, details: Optional[Dict[str, Any]] = None
    ) -> None:
        error = Error(severity, self.name, description, details=details or {})
        self._error_logger.log_error(self.process_id, error)


# Usage example
if __name__ == "__main__":
    db_connection = DatabaseConnection("mongodb://localhost:27017/", "etl_pipeline_logs", "process_tree")
    error_logger = ErrorLogger(db_connection)

    with ProcessContext("ETL_Pipeline", error_logger) as etl:
        etl.log_error(ErrorSeverity.INFO, "ETL process started")
        # Simulating an error
        etl.log_error(ErrorSeverity.ERROR, "Data validation failed", {"invalid_rows": 10})
