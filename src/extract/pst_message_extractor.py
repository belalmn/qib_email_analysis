import logging
import math
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pypff
from pydantic import BaseModel, Field

from src.utils.config import Config
from src.utils.exception_handler import handle_exceptions
from src.utils.pypff_folder_utils import (
    get_folders_from_pst,
    get_number_of_sub_messages,
)
from src.utils.pypff_message_utils import get_provider_email_id_from_message


class PstMessage(BaseModel):
    message: Any
    provider_email_id: int
    folder_name: str


class MessageBatch(BaseModel):
    messages: List[PstMessage]
    batch_id: int


class PstMessageExtractor:
    def __init__(self, pst_path: str, batch_size: int):
        self.pst_path: str = pst_path
        self.batch_size: int = batch_size
        self.pst_folders: Dict[str, pypff.folder] = get_folders_from_pst(self.pst_path)

    def get_total_batches(self, folder_name: Optional[str] = None) -> int:
        total_batches = 0
        if not folder_name:
            for folder_name, folder in self.pst_folders.items():
                if not folder:
                    continue
                if not folder.sub_messages:
                    logging.debug(f"Skipping empty folder: {folder_name}")
                    continue
                logging.debug(f"Extracting messages from folder: {folder_name}")
                total_batches += get_number_of_sub_messages(folder)
        else:
            folder = self.pst_folders.get(folder_name)
            if folder:
                logging.debug(f"Extracting messages from folder: {folder_name}")
                total_batches += get_number_of_sub_messages(folder)
            else:
                logging.error(f"Folder not found: {folder_name}")
        return math.ceil(total_batches / self.batch_size)

    @handle_exceptions("Failed to extract messages from PST", reraise=True)
    def extract_messages(self, folder_name: Optional[str] = None) -> Iterator[MessageBatch]:
        batch_id = 0
        if not folder_name:
            logging.debug("Extracting messages from all folders")
            for folder_name, folder in self.pst_folders.items():
                if not folder:
                    continue
                if not folder.sub_messages:
                    logging.debug(f"Skipping empty folder: {folder_name}")
                    continue
                logging.debug(f"Extracting messages from folder: {folder_name}")
                yield from self._extract_from_folder(folder, folder_name, batch_id)
        else:
            folder = self.pst_folders.get(folder_name)
            if folder:
                logging.debug(f"Extracting messages from folder: {folder_name}")
                yield from self._extract_from_folder(folder, folder_name, batch_id)
            else:
                logging.error(f"Folder not found: {folder_name}")

    @handle_exceptions("Failed to extract messages from folder", reraise=True)
    def _extract_from_folder(
        self, folder: pypff.folder, folder_name: str, batch_id: int
    ) -> Iterator[MessageBatch]:
        batch: List[PstMessage] = []
        for message in folder.sub_messages:
            provider_email_id = get_provider_email_id_from_message(message)
            pst_message = PstMessage(
                message=message, provider_email_id=provider_email_id, folder_name=folder_name
            )
            batch.append(pst_message)
            if len(batch) >= self.batch_size:
                batch_id += 1
                yield MessageBatch(messages=batch, batch_id=batch_id)
                batch = []

        if batch:  # Yield any remaining messages
            batch_id += 1
            yield MessageBatch(messages=batch, batch_id=batch_id)
