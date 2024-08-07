import logging
from datetime import datetime
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

import pypff
from pydantic import BaseModel, Field

from src.utils.exception_handler import handle_exceptions


class ExtractionResult(BaseModel):
    success: bool
    data: Any
    error: Optional[str]
    warning: Optional[str]


@handle_exceptions("Failed to extract number of messages from folder", reraise=True)
def get_number_of_sub_messages(folder: pypff.folder) -> int:
    return folder.get_number_of_sub_messages()


@handle_exceptions("Failed to extract folders from PST", reraise=True)
def get_folders_from_pst(file_path: str) -> Dict[str, pypff.folder]:
    pst: pypff.file = _open_pst_file(file_path)
    outlook_data_file: pypff.folder = _get_outlook_data_file_from_pst(pst)
    folders: Dict[str, pypff.folder] = _get_pypff_folders(outlook_data_file)
    return folders


@handle_exceptions("Failed to open PST file", reraise=True)
def _open_pst_file(file_path: str) -> pypff.file:
    pst: pypff.file = pypff.file()
    pst.open(file_path)
    return pst


@handle_exceptions("Failed to get Outlook data file from PST", reraise=True)
def _get_outlook_data_file_from_pst(pst: pypff.file) -> pypff.folder:
    root: pypff.folder = pst.get_root_folder()
    root_folders: List[pypff.folder] = [
        root.get_sub_folder(i) for i in range(root.get_number_of_sub_folders())
    ]
    data: Dict[str, pypff.folder] = {i.get_name(): i for i in root_folders}
    return data["Top of Outlook data file"]


@handle_exceptions("Failed to extract messages from folder", reraise=True)
def _get_pypff_folders(folder: pypff.folder) -> Dict[str, pypff.folder]:
    folders: List[pypff.folder] = [
        folder.get_sub_folder(i) for i in range(folder.get_number_of_sub_folders())
    ]
    return {i.get_name(): i for i in folders}
