import logging
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pypff
from pydantic import BaseModel, Field

from src.utils.config import Config
from src.utils.exception_handler import handle_exceptions
from src.utils.pypff_folder_utils import get_folders_from_pst
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

    @handle_exceptions("Failed to extract messages from PST", reraise=True)
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
            yield from self._extract_from_folder(folder, folder_name, batch_id)

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
