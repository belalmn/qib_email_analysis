import logging
import re
from datetime import datetime
from typing import List, Optional, Union

import pypff
from pydantic import BaseModel, field_validator, model_validator
from pyisemail import is_email

from src.utils.exception_handler import handle_exceptions
from src.utils.pypff_message_utils import PypffMessage


class ParsedMessage(BaseModel):
    folder_name: str
    provider_email_id: int
    global_message_id: str
    creation_time: datetime
    submit_time: datetime
    delivery_time: datetime
    from_address: str
    to_address: Optional[Union[List[str], str]]
    cc_address: Optional[Union[List[str], str]]
    bcc_address: Optional[Union[List[str], str]]
    sender_name: Optional[str]
    in_reply_to: Optional[str]
    subject: Optional[str]
    body: Optional[str]

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

    @model_validator(mode="after")
    @classmethod
    @handle_exceptions("Failed to validate message")
    def validate_message(cls, parsed_message: "ParsedMessage") -> None:
        if (
            parsed_message.to_address is None
            and parsed_message.cc_address is None
            and parsed_message.bcc_address is None
        ):
            logging.error(f"Message {parsed_message.provider_email_id} has no recipients")
            raise ValueError("Message has no recipients")


class MessageParser:
    @staticmethod
    @handle_exceptions("Failed to parse message")
    def parse(message: pypff.message, provider_email_id: int, folder_name: str) -> ParsedMessage:
        message = PypffMessage(message)
        try:
            return ParsedMessage(
                provider_email_id=message.get_value("provider_email_id"),
                subject=message.get_value("subject"),
                sender_name=message.get_value("sender_name"),
                creation_time=message.get_value("creation_time"),
                submit_time=message.get_value("submit_time"),
                delivery_time=message.get_value("delivery_time"),
                body=message.get_value("body"),
                folder_name=folder_name,
                from_address=message.get_value("from_address"),
                to_address=message.get_value("to_address"),
                cc_address=message.get_value("cc_address"),
                bcc_address=message.get_value("bcc_address"),
                in_reply_to=message.get_value("in_reply_to"),
                global_message_id=message.get_value("global_message_id"),
            )
        except ValueError as e:
            logging.error(f"Error parsing message {provider_email_id}: {e}")
            raise
