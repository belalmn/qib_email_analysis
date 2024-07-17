from typing import Dict, List

import pypff
import pprint

from src.utils.exception_handler import handle_exceptions


class PST_DEBUG_TOOL:
    def __init__(self, pst_path: str):
        self.pst_path: str = pst_path
        self.pst: pypff.file = pypff.file()
        self.pst.open(pst_path)
        self.root_folder: pypff.folder = self.pst.get_root_folder()
        self.root_folders: List[pypff.folder] = [
            self.root_folder.get_sub_folder(i)
            for i in range(self.root_folder.get_number_of_sub_folders())
        ]
        self.root_folder_dict: Dict[str, pypff.folder] = {i.get_name(): i for i in self.root_folders}
        self.top_of_outlook_data_file: pypff.folder = self.root_folder_dict["Top of Outlook data file"]
        self.sub_folders: List[pypff.folder] = [
            self.top_of_outlook_data_file.get_sub_folder(i)
            for i in range(self.top_of_outlook_data_file.get_number_of_sub_folders())
        ]
        self.sub_folder_dict: Dict[str, pypff.folder] = {i.get_name(): i for i in self.sub_folders}
        self.inbox: pypff.folder = self.sub_folder_dict["Inbox"]
        self.messages: List[pypff.message] = [
            self.inbox.get_sub_message(i) for i in range(self.inbox.get_number_of_sub_messages())
        ]
        self.message_dict: Dict[int, pypff.message] = {i.get_identifier(): i for i in self.messages}

    @handle_exceptions("Failed to get message from provider email ID", reraise=True)
    def print_headers_from_id(self, provider_email_id: int) -> pypff.message:
        headers = self.message_dict[provider_email_id].get_transport_headers()
        pprint.pprint(headers)

