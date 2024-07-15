import logging
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pypff
from pydantic import BaseModel, Field

from src.utils.config import Config
from src.utils.pypff_folder_utils import get_folders_from_pst


class PstMessage(BaseModel):
    message: Any
    folder_name: str


class MessageBatch(BaseModel):
    messages: List[PstMessage]
    batch_id: int


class ExtractionResult(BaseModel):
    success: bool
    data: Any
    error: Optional[str]
    warning: Optional[str]


class PstExtractionError(Exception):
    def __init__(self, description: str, folder_name: str, batch_id: int, original_error: Exception):
        self.description = description
        self.folder_name = folder_name
        self.batch_id = batch_id
        self.original_error = original_error
        self.timestamp = datetime.now()

    def __str__(self):
        return (
            f"PstExtractError: {self.description}\n"
            f"Folder: {self.folder_name}\n"
            f"Batch ID: {self.batch_id}\n"
            f"Original Error: {self.original_error}\n"
            f"Timestamp: {self.timestamp}"
        )


class PstMessageExtractor:
    def __init__(self, pst_path: str, batch_size: int):
        self.pst_path: str = pst_path
        self.batch_size: int = batch_size
        self.errors: List[PstExtractionError] = []

    def extract_messages(self) -> Iterator[MessageBatch]:
        pst_folders: Dict[str, pypff.folder] = get_folders_from_pst(self.pst_path)
        batch_id = 0
        for folder_name, folder in pst_folders.items():
            if not folder:
                continue
            if not folder.sub_messages:
                logging.info(f"Skipping empty folder: {folder_name}")
                continue
            logging.info(f"Extracting messages from folder: {folder_name}")
            try:
                yield from self._extract_from_folder(folder, folder_name, batch_id)
            except Exception as e:
                self._log_error("Error processing folder", folder_name, batch_id, e)

    def _extract_from_folder(
        self, folder: pypff.folder, folder_name: str, batch_id: int
    ) -> Iterator[MessageBatch]:
        batch: List[PstMessage] = []
        for message in folder.sub_messages:
            try:
                pst_message = PstMessage(message=message, folder_name=folder_name)
                batch.append(pst_message)
                if len(batch) >= self.batch_size:
                    batch_id += 1
                    yield MessageBatch(messages=batch, batch_id=batch_id)
                    batch = []
            except Exception as e:
                self._log_error("Error extracting individual message", folder_name, batch_id, e)
                continue

        if batch:  # Yield any remaining messages
            batch_id += 1
            yield MessageBatch(messages=batch, batch_id=batch_id)

    def _log_error(self, description: str, folder_name: str, batch_id: int, error: Exception):
        self.errors.append(
            PstExtractionError(
                description=description,
                folder_name=folder_name,
                batch_id=batch_id,
                original_error=error,
            )
        )

    def get_errors(self) -> List[PstExtractionError]:
        return self.errors
