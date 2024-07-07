from typing import Any, List, Dict, Optional, Union
from datetime import datetime
import pypff
from pydantic import BaseModel, Field
import email
import logging
from src.extract.pst_extractor import EmailMessage

class PrimaryFeaturesExtractor:
    @staticmethod
    def extract(message: pypff.message, folder_name: str) -> EmailMessage:
        headers = email.message_from_string(message.transport_headers or "")
        attachment_count: int = 0

        try:
            attachment_count = message.get_number_of_attachments()
        except Exception as e:
            #logging.warning(f"Error extracting attachments: {e}")
            pass

        return EmailMessage(
            identifier=str(message.identifier),
            subject=message.subject or "",
            sender_name=message.sender_name or "",
            from_address=headers.get('From'),
            to_address=headers.get('To'),
            cc_address=headers.get('CC'),
            bcc_address=headers.get('BCC'),
            creation_time=message.creation_time,
            submit_time=message.client_submit_time,
            delivery_time=message.delivery_time,
            attachment_count=attachment_count,
            body=PrimaryFeaturesExtractor._truncate_body(message.plain_text_body),
            folder_name=folder_name
        )

    @staticmethod
    def _truncate_body(body: str, max_length: int = 10000) -> str:
        return (body or "")[:max_length]