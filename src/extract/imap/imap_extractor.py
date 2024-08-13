import email
import imaplib
import logging
import re
import time
from datetime import datetime
from email.header import decode_header
from email.parser import BytesParser
from email.policy import default
from email.utils import formatdate, getaddresses
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from tqdm.auto import tqdm

from src.extract.imap.imap_parsing_utils import decode_str, parse_timestamp
from src.extract.shared.parsing_utils import (
    charset_from_content_type,
    parse_addresses,
    parse_domain_info,
    parse_email_threading,
    parse_identifiers,
    prefix_from_subject,
)

logging.basicConfig(level=logging.INFO)


class IMAPExtractor:
    def __init__(self, username: str, password: str, server: str = "imap.gmail.com"):
        self.username = username
        self.password = password
        self.server = server
        self.imap = imaplib.IMAP4_SSL(self.server)
        self.imap.login(self.username, self.password)

    def __del__(self):
        self.close()

    def close(self):
        try:
            self.imap.logout()
        except:
            pass

    def list_mailboxes(self) -> None:
        print("Available mailboxes:")
        for i, folder_info in enumerate(self.imap.list()[1], 1):
            if not isinstance(folder_info, bytes):
                continue
            folder = folder_info.decode().split('"')[1]
            print(f"{i}. {folder}")

    def extract_messages_from_imap(
        self,
        folders: List[str],
        message_ids: Optional[Set[str]] = None,
        since: Optional[datetime] = None,
    ) -> pd.DataFrame:

        emails_list = []
        start_time = time.time()
        found_message_ids: Set[str] = set()

        for folder in folders:
            self.imap.select(folder, readonly=True)

            if message_ids:
                remaining_message_ids = message_ids - found_message_ids
                emails_list.extend(self.fetch_emails_by_message_ids(remaining_message_ids)[0])
                found_message_ids.update(remaining_message_ids)
            elif since:
                emails_list.extend(self.fetch_emails_since_date(since))
            else:
                raise ValueError("Either message_ids or since must be provided")

        emails_df = pd.DataFrame(emails_list)
        emails_df = parse_email_threading(emails_df)
        emails_df = parse_domain_info(emails_df)

        processing_time = time.time() - start_time
        total_emails = len(emails_df)
        found_message_ids_count = len(found_message_ids)
        requested_message_ids_count = len(message_ids) if message_ids else 0

        logging.info(f"Total processing time: {processing_time:.2f} seconds")
        logging.info(f"Retrieved {total_emails} emails")
        if message_ids:
            logging.info(
                f"Found {found_message_ids_count} out of {requested_message_ids_count} requested message IDs"
            )

        return emails_df

    def fetch_emails_by_message_ids(self, message_ids: Set[str]) -> Tuple[List[Dict[str, Any]], Set[str]]:
        fetched_emails = []
        found_message_ids = set()

        for message_id in tqdm(message_ids, desc="Fetching emails by Message-ID", leave=False):
            try:
                cleaned_message_id = message_id.strip("<>")
                search_query = f'HEADER Message-ID "{cleaned_message_id}"'
                result, data = self.imap.search(None, search_query)

                if result == "OK" and data and data[0] and data[0].decode():
                    email_ids = data[0].decode().split()
                    for email_id in email_ids:
                        email_data = self.fetch_and_parse_email(email_id)
                        if email_data:
                            fetched_emails.append(email_data)
                            found_message_ids.add(cleaned_message_id)
                            break
            except Exception as e:
                logging.error(f"Error fetching email with Message-ID {message_id}: {e}")

        return fetched_emails, found_message_ids

    def fetch_emails_since_date(self, since: datetime) -> List[Dict[str, Any]]:
        fetched_emails = []
        date_string = since.strftime("%d-%b-%Y")  # Format date as "01-Jan-2023"

        try:
            search_query = f'SINCE "{date_string}"'
            result, data = self.imap.search(None, search_query)

            if result == "OK" and data and data[0] and data[0].decode():
                email_ids = data[0].decode().split()
                for email_id in tqdm(email_ids, desc="Fetching emails", leave=False):
                    email_data = self.fetch_and_parse_email(email_id)
                    if email_data:
                        fetched_emails.append(email_data)
        except Exception as e:
            logging.error(f"Error fetching emails since {date_string}: {str(e)}")

        return fetched_emails

    def fetch_and_parse_email(self, email_id: str) -> Optional[Dict[str, Any]]:
        try:
            result, msg_data = self.imap.fetch(email_id, "(RFC822)")
            if result == "OK" and isinstance(msg_data[0], tuple):
                raw_email = msg_data[0][1]
                msg = BytesParser(policy=default).parsebytes(raw_email)
                return self.parse_email(msg)
        except Exception as e:
            logging.error(f"Error fetching and parsing email {email_id}: {str(e)}")
        return None

    def parse_email(self, email: email.message.Message) -> Dict[str, Any]:
        # Metadata
        from_name = decode_str(email["From"].split("<")[0])
        subject = decode_str(email["Subject"])
        subject_prefix = prefix_from_subject(subject)

        # Timestamps
        submit_time = parse_timestamp(email["Date"])
        delivery_time = parse_timestamp(email["Received"].split(";")[-1].strip())

        # Headers
        content_type = email.get_content_type()
        charset = charset_from_content_type(content_type)

        # Identifiers
        message_id = parse_identifiers(email["Message-ID"])
        previous_message_id = parse_identifiers(email["In-Reply-To"])
        references = parse_identifiers(email["References"])

        # Addresses
        from_address = parse_addresses(email["From"])
        to_address = parse_addresses(email["To"])
        cc_address = parse_addresses(email["CC"])
        bcc_address = parse_addresses(email["BCC"])

        # Body types
        html_body = ""
        plain_text_body = ""

        if email.is_multipart():
            for part in email.walk():
                if part.get_content_type() == "text/plain":
                    plain_text_body += part.get_payload(decode=True).decode(errors="ignore")
                elif part.get_content_type() == "text/html":
                    html_body += part.get_payload(decode=True).decode(errors="ignore")
        else:
            plain_text_body = email.get_payload(decode=True).decode(errors="ignore")

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
