import pypff
from typing import Iterator, List, Any
from pydantic import BaseModel, Field

class MessageChunk(BaseModel):
    messages: List[Any] = Field(description="List of pypff message objects") # TODO: Possibly replace with more optimized data structure (stack, queue, etc.)
    folder_path: str = Field(description="Path of the folder containing the messages")

class PstMessageExtractor:
    def __init__(self, file_path: str, chunk_size: int = 100):
        self.file_path: str = file_path
        self.chunk_size: int = chunk_size

    def extract_messages(self) -> Iterator[MessageChunk]:
        pst: pypff.file = pypff.file()
        pst.open(self.file_path)
        root: pypff.folder = pst.get_root_folder()
        outlook_data_file: pypff.data_file = root.get_sub_folder('Top of Outlook data file')

        for folder in self._traverse_folders(outlook_data_file):
            chunk: List[Any] = []
            folder_path: str = self._get_folder_path(folder)
            
            for message in folder.sub_messages:
                chunk.append(message)
                if len(chunk) >= self.chunk_size:
                    yield MessageChunk(messages=chunk, folder_path=folder_path)
                    chunk = []
            
            if chunk:  # Yield any remaining messages
                yield MessageChunk(messages=chunk, folder_path=folder_path)

    def _traverse_folders(self, folder: pypff.folder) -> Iterator[pypff.folder]:
        yield folder
        for subfolder in folder.sub_folders:
            yield from self._traverse_folders(subfolder)

    def _get_folder_path(self, folder: pypff.folder) -> str:
        path = []
        current = folder
        while current:
            path.append(current.name)
            current = current.parent
        return " / ".join(reversed(path))