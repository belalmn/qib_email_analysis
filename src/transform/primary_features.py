from typing import Any, Dict, Optional
from datetime import datetime
import pypff
from pydantic import BaseModel, Field
import email

class PrimaryFeatures(BaseModel):
    identifier: str
    subject: str
    sender_name: str
    transport_headers: str
    from_address: Optional[str]
    to_address: Optional[str]
    cc_address: Optional[str]
    bcc_address: Optional[str]
    creation_time: datetime
    submit_time: datetime
    delivery_time: datetime
    attachment_count: int
    body: str = Field(..., max_length=1000)  # Truncate to 1000 characters
    folder_name: str

class PrimaryFeaturesExtractor:
    @staticmethod
    def extract(message: pypff.message, folder_name: str) -> PrimaryFeatures:
        headers = email.message_from_string(message.transport_headers or "")
        
        return PrimaryFeatures(
            identifier=str(message.identifier),
            subject=message.subject or "",
            sender_name=message.sender_name or "",
            transport_headers=message.transport_headers or "",
            from_address=headers.get('From'),
            to_address=headers.get('To'),
            cc_address=headers.get('CC'),
            bcc_address=headers.get('BCC'),
            creation_time=datetime.fromtimestamp(message.creation_time),
            submit_time=datetime.fromtimestamp(message.client_submit_time),
            delivery_time=datetime.fromtimestamp(message.delivery_time),
            attachment_count=message.number_of_attachments,
            body=PrimaryFeaturesExtractor._truncate_body(message.plain_text_body),
            folder_name=folder_name
        )

    @staticmethod
    def _truncate_body(body: str, max_length: int = 1000) -> str:
        return (body or "")[:max_length]