import logging
import re
from datetime import datetime
from typing import List, Optional, Union

import pypff
from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)
from typing_extensions import Self

from src.utils.pypff_message_utils import PypffMessage

MAX_BODY_LENGTH = 10000


class ParsedMessage(BaseModel):
    folder_name: str
    identifier: int
    subject: Optional[str]
    sender_name: Optional[str]
    creation_time: datetime
    submit_time: datetime
    delivery_time: datetime
    body: Optional[str] = Field(max_length=MAX_BODY_LENGTH)
    from_address: str
    to_address: Optional[Union[List[str], str]]
    cc_address: Optional[Union[List[str], str]]
    bcc_address: Optional[Union[List[str], str]]
    in_reply_to: Optional[str]
    message_id: Optional[str]

    @field_validator("from_address", "to_address", "cc_address", "bcc_address")
    @classmethod
    def validate_email(cls, v: Optional[Union[str, List[str]]]) -> Optional[Union[str, List[str]]]:
        if v is None:
            return v
        email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if isinstance(v, str):
            if not re.match(email_regex, v):
                raise ValueError(f"Invalid email address: {v}")
        elif isinstance(v, list):
            for email in v:
                if not re.match(email_regex, email):
                    raise ValueError(f"Invalid email address in list: {email}")
        return v

    # @model_validator(mode="after")
    # def validate_time_order(self) -> Self:
    #     if self.submit_time > self.delivery_time:
    #         raise ValueError(
    #             f"Delivery time {self.delivery_time} cannot be before submit time {self.submit_time}"
    #         )
    #     if self.submit_time > self.creation_time:
    #         raise ValueError(
    #             f"Creation time {self.creation_time} cannot be before submit time {self.submit_time}"
    #         )
    #     if self.delivery_time > self.creation_time:
    #         raise ValueError(
    #             f"Creation time {self.creation_time} cannot be before delivery time {self.delivery_time}"
    #         )
    #     return self

    @field_validator("body")
    @classmethod
    def truncate_body(cls, v: str) -> str:
        return v[:MAX_BODY_LENGTH] if v else ""


class MessageParser:
    @staticmethod
    def extract(message: pypff.message, folder_name: str) -> ParsedMessage:
        message = PypffMessage(message)

        try:
            id = message.get_value("identifier")
            message_id = message.get_value("message_id")
        except Exception as e:
            pass

        try:
            return ParsedMessage(
                identifier=message.get_value("identifier"),
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
                message_id=message.get_value("message_id"),
            )
        except ValueError as e:
            logging.error(f"Error parsing message {id}: {e}")
            raise
