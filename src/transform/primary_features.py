from datetime import datetime
from typing import List, Optional, Union

import pypff
from pydantic import BaseModel, Field

from src.utils.pypff_message_utils import PypffMessage


class PrimaryEmailFeatures(BaseModel):
    identifier: int
    subject: Optional[str]
    sender_name: str
    from_address: Optional[str]
    to_address: Optional[Union[List[str], str]]
    cc_address: Optional[Union[List[str], str]]
    bcc_address: Optional[Union[List[str], str]]
    creation_time: datetime
    submit_time: datetime
    delivery_time: datetime
    body: str = Field(..., max_length=10000)  # Truncate to 1000 characters
    folder_name: str


class PrimaryFeaturesExtractor:
    @staticmethod
    def extract(message: pypff.message, folder_name: str) -> PrimaryEmailFeatures:
        message = PypffMessage(message)

        return PrimaryEmailFeatures(
            identifier=message.get_value("identifier"),
            subject=message.get_value("subject"),
            sender_name=message.get_value("sender_name"),
            from_address=message.get_value("from_address"),
            to_address=message.get_value("to_address"),
            cc_address=message.get_value("cc_address"),
            bcc_address=message.get_value("bcc_address"),
            creation_time=message.get_value("creation_time"),
            submit_time=message.get_value("submit_time"),
            delivery_time=message.get_value("delivery_time"),
            body=PrimaryFeaturesExtractor._truncate_body(message.get_value("body")),
            folder_name=folder_name,
        )

    @staticmethod
    def _truncate_body(body: str, max_length: int = 10000) -> str:
        return (body or "")[:max_length]
