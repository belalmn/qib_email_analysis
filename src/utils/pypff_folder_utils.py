import email
import logging
from typing import Any, Dict, Iterator, List

import pypff
from pydantic import BaseModel, Field

def get_folders_from_pst(file_path: str) -> Dict[str, pypff.folder]:
    pst: pypff.file = _open_pst_file(file_path)
    if not pst:
        return {}
    outlook_data_file: pypff.folder = _get_outlook_data_file_from_pst(pst)
    folders: Dict[str, pypff.folder] = _get_pypff_folders(outlook_data_file)
    return folders

def _get_outlook_data_file_from_pst(pst: pypff.file) -> pypff.folder:
    root: pypff.folder = pst.get_root_folder()
    root_folders: List[pypff.folder] = [
        root.get_sub_folder(i) for i in range(root.get_number_of_sub_folders())
    ]
    data: Dict[str, pypff.folder] = {i.get_name(): i for i in root_folders}
    return data["Top of Outlook data file"]


def _open_pst_file(file_path: str) -> pypff.file:
    pst: pypff.file = pypff.file()
    try:
        pst.open(file_path)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"PST file not found: {e}")
    return pst


def _get_pypff_folders(folder: pypff.folder) -> Dict[str, pypff.folder]:
    folders: List[pypff.folder] = [
        folder.get_sub_folder(i) for i in range(folder.get_number_of_sub_folders())
    ]
    return {i.get_name(): i for i in folders}