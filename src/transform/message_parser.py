import logging
import re
from datetime import datetime
from typing import List, Optional, Union

import pypff
from pydantic import BaseModel, field_validator, model_validator
from pyisemail import is_email

from src.pst.pypff_message import PypffMessage
from src.utils.exception_handler import handle_exceptions


class ParsedMessage(BaseModel):
    folder_name: str
    provider_email_id: int
    global_message_id: str
    creation_time: datetime
    submit_time: datetime
    delivery_time: datetime
    from_address: str
    content_type: Optional[str] = None
    to_address: Optional[Union[List[str], str]] = None
    cc_address: Optional[Union[List[str], str]] = None
    bcc_address: Optional[Union[List[str], str]] = None
    sender_name: Optional[str] = None
    in_reply_to: Optional[str] = None
    subject: Optional[str] = None
    plain_text_body: Optional[str] = None
    rich_text_body: Optional[str] = None
    html_body: Optional[str] = None
    references: Optional[List[str]] = None

    @field_validator("to_address", "cc_address", "bcc_address")
    @classmethod
    @handle_exceptions("Failed to validate email addresses")
    def validate_email(cls, v: Optional[Union[str, List[str]]]) -> Optional[Union[str, List[str]]]:
        if v:
            if isinstance(v, list):
                for email in v:
                    if not is_email(email):
                        logging.error(f"Invalid email address: {email}")
                        raise
            else:
                if not is_email(v):
                    logging.error(f"Invalid email address: {v}")
                    raise
        return v

    @field_validator("from_address")
    @classmethod
    @handle_exceptions("Failed to validate from address")
    def validate_from_address(cls, v: str) -> str:
        if not is_email(v):
            logging.error(f"Invalid email address: {v}")
            raise
        return v


class MessageParser:
    @staticmethod
    @handle_exceptions("Failed to parse message")
    def parse(message: pypff.message, folder_name: str) -> ParsedMessage:
        pypff_message = PypffMessage(message)
        parsed_data = pypff_message.parsed_data
        parsed_data["folder_name"] = folder_name
        try:
            return ParsedMessage(**parsed_data)
        except Exception as e:
            logging.error(f"Failed to parse message: {e}")
            raise
