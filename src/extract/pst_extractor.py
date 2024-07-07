import logging
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Union

import pypff
from pydantic import BaseModel, Field

from src.utils.config import Config
from src.utils.pypff_folder_utils import get_folders_from_pst
from src.transform.primary_features import PrimaryEmailFeatures


class MessageChunk(BaseModel):
    messages: List[Any]
    folder_path: str


class ProcessedBatch(BaseModel):
    batch_id: str
    processed_at: datetime
    messages: List[PrimaryEmailFeatures]


class PstMessageExtractor:
    def __init__(self, pst_path: str, chunk_size: int):
        self.pst_path: str = pst_path
        self.chunk_size: int = chunk_size

    def extract_messages(self) -> Iterator[MessageChunk]:
        pst_folders: Dict[str, pypff.folder] = get_folders_from_pst(self.pst_path)
        for folder_name, folder in pst_folders.items():
            chunk: List[Any] = []
            if not folder.sub_messages:
                logging.info(f"Skipping empty folder: {folder_name}")
                continue
            logging.info(f"Extracting messages from folder: {folder_name}")
            for message in folder.sub_messages:
                chunk.append(message)
                if len(chunk) >= self.chunk_size:
                    yield MessageChunk(messages=chunk, folder_path=folder_name)
                    chunk = []

            if chunk:  # Yield any remaining messages
                yield MessageChunk(messages=chunk, folder_path=folder_name)
