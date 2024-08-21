import logging
import multiprocessing as mp
from multiprocessing.pool import ThreadPool as Pool
from typing import Any, Dict, List, Optional, Set, Union

import html2text
import pandas as pd
import pypff
from tqdm import tqdm

from src.extract.parsing_utils import (
    charset_from_content_type,
    fill_plain_text_body,
    parse_addresses,
    parse_body,
    parse_domain_info,
    parse_email_threading,
    parse_identifiers,
    prefix_from_subject,
)
from src.extract.pst_parsing_utils import parse_headers, parse_timestamp, safe_getattr

htmlConverter = html2text.HTML2Text()
htmlConverter.ignore_links = True
htmlConverter.ignore_images = True
htmlConverter.ignore_emphasis = True

tqdm.pandas()


class PSTExtractor:
    def __init__(
        self,
        file_paths: Union[str, List[str]],
        sample: Optional[int] = None,
        fill_missing_data: bool = False,
    ):
        self.file_paths = file_paths if isinstance(file_paths, list) else [file_paths]
        self.sample = sample
        self.messages = []

        for file_path in self.file_paths:
            logging.info(f"Opening {file_path} for extraction")
            folder = self.retrieve_folder(file_path)
            if folder:
                logging.info(f"Extracting messages from {file_path}")
                messages = self.extract_messages_from_pst(folder)
                self.messages.extend(messages)

        logging.info(f"Found {len(self.messages)} messages in total")

        if self.sample:
            logging.info(f"Sampling {self.sample} messages")
            self.messages = self.messages[: self.sample]

        logging.info("Parsing messages")
        message_df = self.parse_messages(self.messages)

        if fill_missing_data:
            logging.info("Filling missing data")
            message_df = fill_plain_text_body(message_df)

        logging.info("Parsing email threading")
        message_df = parse_email_threading(message_df)

        logging.info("Parsing domain info")
        message_df = parse_domain_info(message_df)

        logging.info("Extracting missing email ids")
        self.missing_email_ids = self.get_missing_message_ids(message_df)

        self.message_df = message_df
        logging.info(f"Extracted {len(message_df)} messages")

    def open_pst_file(self, file_path: str) -> pypff.file:
        pst: pypff.file = pypff.file()
        pst.open(file_path)
        return pst

    def get_outlook_data_file_from_pst(self, pst: pypff.file) -> pypff.folder:
        root: pypff.folder = pst.get_root_folder()
        root_folders: List[pypff.folder] = [
            root.get_sub_folder(i) for i in range(root.get_number_of_sub_folders())
        ]
        data: Dict[str, pypff.folder] = {i.get_name(): i for i in root_folders}
        return data["Top of Outlook data file"]

    def get_pypff_folders(self, folder: pypff.folder) -> Dict[str, pypff.folder]:
        folders: List[pypff.folder] = [
            folder.get_sub_folder(i) for i in range(folder.get_number_of_sub_folders())
        ]
        folders.append(folder)
        return {i.get_name(): i for i in folders}

    def retrieve_message(self, message: pypff.message) -> pypff.message:
        return message

    def retrieve_folder(self, file_path: str) -> Optional[pypff.folder]:
        try:
            pst: pypff.file = self.open_pst_file(file_path)
            outlook_data_file: pypff.folder = self.get_outlook_data_file_from_pst(pst)
            folders: Dict[str, pypff.folder] = self.get_pypff_folders(outlook_data_file)

            folder_info = sorted([(name, len(folder.sub_messages)) for name, folder in folders.items()], key=lambda x: x[1], reverse=True)
            logging.info(f"Found {len(folders)} folders:\n\t" + "\n\t".join([f"{name}: {count} messages" for name, count in folder_info]))

            if "Inbox" in folders:
                return folders["Inbox"]
            elif len(outlook_data_file.sub_messages) > 0:
                logging.info(f"Using Outlook data file folder")
                return outlook_data_file
            else:
                logging.error("Inbox folder not found, and Outlook data file folder is empty")
                return None
        except Exception as e:
            logging.error(f"Error retrieving folder from {file_path}: {e}")
            return None

    def extract_messages_from_pst(self, folder: pypff.folder) -> List[pypff.message]:
        sub_messages = list(folder.sub_messages)
        total = len(sub_messages)

        num_processes = mp.cpu_count()
        chunk_size = int(total / num_processes)

        logging.info(f"Using {num_processes} processes with a chunk size of {chunk_size}")

        with Pool(processes=num_processes) as pool:
            with tqdm(total=total) as pbar:
                message_array = []
                for message in pool.imap_unordered(
                    self.retrieve_message, sub_messages, chunksize=chunk_size
                ):
                    message_array.append(message)
                    pbar.update(1)

        return message_array

    def get_missing_message_ids(self, message_df: pd.DataFrame) -> Set[str]:
        existing_message_ids = set(message_df["message_id"].values)
        missing_message_ids: Set[str] = set()
        for _, row in message_df.iterrows():
            if (
                (row["references"] is None and row["previous_message_id"] is None)
                or row["subject_prefix"]
                and "fw" in row["subject_prefix"]
            ):
                continue
            if row["references"]:
                for reference in row["references"].split(","):
                    if reference not in existing_message_ids:
                        missing_message_ids.add(reference)
            if row["previous_message_id"] and row["previous_message_id"] not in existing_message_ids:
                missing_message_ids.add(row["previous_message_id"])

        return missing_message_ids

    def parse_message(self, message: pypff.message) -> Dict[str, Any]:
        # Metadata
        from_name = safe_getattr(message, "sender_name")
        subject = safe_getattr(message, "subject")
        subject_prefix = prefix_from_subject(subject)

        # Timestamps
        submit_time = parse_timestamp(safe_getattr(message, "get_client_submit_time_as_integer"))
        delivery_time = parse_timestamp(safe_getattr(message, "get_delivery_time_as_integer"))

        # Headers
        headers = parse_headers(message)
        content_type = headers.get("content-type", None)
        charset = charset_from_content_type(content_type)

        # Identifiers
        message_id = parse_identifiers(headers.get("message-id", None))
        previous_message_id = parse_identifiers(headers.get("in-reply-to", None))
        references = parse_identifiers(headers.get("references", None))

        # Addresses
        from_address = parse_addresses(headers.get("from", None))
        to_address = parse_addresses(headers.get("to", None))
        cc_address = parse_addresses(headers.get("cc", None))
        bcc_address = parse_addresses(headers.get("bcc", None))

        # Body types
        html_body = parse_body(safe_getattr(message, "html_body"), charset)
        plain_text_body = parse_body(safe_getattr(message, "plain_text_body"), charset)

        return {
            "message_id": message_id,
            "subject": subject,
            "subject_prefix": subject_prefix,
            "submit_time": submit_time,
            "delivery_time": delivery_time,
            "html_body": html_body,
            "plain_text_body": plain_text_body,
            "from_name": from_name,
            "from_address": from_address,
            "to_address": to_address,
            "cc_address": cc_address,
            "bcc_address": bcc_address,
            "previous_message_id": previous_message_id,
            "references": references,
        }

    def parse_messages(self, messages: List[pypff.message]) -> pd.DataFrame:
        num_processes = mp.cpu_count()
        chunk_size = max(1, len(messages) // num_processes)

        logging.info(f"Using {num_processes} processes with a chunk size of {chunk_size}")

        results = []
        with Pool(processes=num_processes) as pool:
            total = len(messages)
            with tqdm(total=total) as pbar:
                for result in pool.imap_unordered(self.parse_message, messages, chunksize=chunk_size):
                    results.append(result)
                    pbar.update(1)

        return pd.DataFrame(results)
