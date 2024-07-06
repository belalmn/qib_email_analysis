import pypff  # type: ignore
from typing import Iterator, List, Any
from pydantic import BaseModel, Field
from utils.pypff_utils import get_folders_from_pst
import logging

class EmailMessage(BaseModel):
    identifier: str
    folder_name: str
    subject: str
    sender_name: str
    transport_headers: str
    from_address: str
    to_address: str
    cc_address: str
    bcc_address: str
    creation_time: str
    submit_time: str
    delivery_time: str
    attachment_count: int
    body: str

class MessageChunk(BaseModel):
    messages: List[EmailMessage]
    folder_path: str

class PstMessageExtractor:
    def __init__(self, pst_path: str, chunk_size: int = 100):
        self.pst_path: str = pst_path
        self.chunk_size: int = chunk_size

    def extract_messages(self) -> Iterator[MessageChunk]:
        pst_folders: Iterator[pypff.folder] = get_folders_from_pst(self.pst_path)
        for folder in pst_folders:
            chunk: List[Any] = []
            folder_path: str = self._get_folder_path(folder)
            print(type(folder))
            if not folder.sub_messages:
                continue
            logging.info(f"Extracting messages from folder: {folder_path}")
            for message in folder.sub_messages:
                chunk.append(message)
                if len(chunk) >= self.chunk_size:
                    yield MessageChunk(messages=chunk, folder_path=folder_path)
                    chunk = []
            
            if chunk:  # Yield any remaining messages
                yield MessageChunk(messages=chunk, folder_path=folder_path)

    def _get_folder_path(self, folder: pypff.folder) -> str:
        path = []
        current = folder
        while current:
            path.append(current.name)
            current = current.parent
        return " / ".join(reversed(path))