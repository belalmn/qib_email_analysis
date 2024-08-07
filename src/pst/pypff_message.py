import logging
import re
from datetime import datetime, timedelta, timezone
from email.message import Message
from email.parser import HeaderParser
from email.utils import getaddresses
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import chardet
import pypff
from pydantic import BaseModel, Field

from src.utils.exception_handler import handle_exceptions


class ValueType(Enum):
    MESSAGE = "message"
    HEADER = "header"
    ADDRESS = "address"
    TIMESTAMP = "timestamp"
    MESSAGE_ID = "message_id"
    BODY = "body"
    REFERENCES = "references"


class MessageField(BaseModel):
    key: str
    value_type: ValueType
    pypff_key: str
    default: Any = None


class PypffMessage:
    FIELDS = [
        MessageField(key="provider_email_id", value_type=ValueType.MESSAGE, pypff_key="identifier"),
        MessageField(key="subject", value_type=ValueType.MESSAGE, pypff_key="conversation_topic"),
        MessageField(key="sender_name", value_type=ValueType.MESSAGE, pypff_key="sender_name"),
        MessageField(key="creation_time", value_type=ValueType.TIMESTAMP, pypff_key="creation_time"),
        MessageField(key="submit_time", value_type=ValueType.TIMESTAMP, pypff_key="client_submit_time"),
        MessageField(key="delivery_time", value_type=ValueType.TIMESTAMP, pypff_key="delivery_time"),
        MessageField(key="html_body", value_type=ValueType.BODY, pypff_key="html_body"),
        MessageField(key="plain_text_body", value_type=ValueType.BODY, pypff_key="plain_text_body"),
        MessageField(key="rich_text_body", value_type=ValueType.BODY, pypff_key="rtf_body"),
        MessageField(key="content_type", value_type=ValueType.HEADER, pypff_key="content-type"),
        MessageField(key="from_address", value_type=ValueType.ADDRESS, pypff_key="from"),
        MessageField(key="to_address", value_type=ValueType.ADDRESS, pypff_key="to"),
        MessageField(key="cc_address", value_type=ValueType.ADDRESS, pypff_key="cc"),
        MessageField(key="bcc_address", value_type=ValueType.ADDRESS, pypff_key="bcc"),
        MessageField(key="previous_message_id", value_type=ValueType.MESSAGE_ID, pypff_key="in-reply-to"),
        MessageField(key="global_message_id", value_type=ValueType.MESSAGE_ID, pypff_key="message-id"),
        MessageField(key="references", value_type=ValueType.REFERENCES, pypff_key="references"),
    ]

    HEADER_PARSER = HeaderParser()

    def __init__(self, message: pypff.message):
        self.message = message
        self.headers = self._parse_headers()
        self.parsed_data = self._parse_message()

    def _parse_headers(self) -> Dict[str, str]:
        headers = self.message.transport_headers or ""
        parsed_headers = self.HEADER_PARSER.parsestr(headers)
        return {k.lower(): v for k, v in parsed_headers.items()} if parsed_headers else {}

    def _parse_message(self) -> Dict[str, Any]:
        return {field.key: self._get_value(field) for field in self.FIELDS}

    def _get_value(self, field: MessageField) -> Any:
        method_map = {
            ValueType.MESSAGE: self._get_message_value,
            ValueType.HEADER: self._get_header_value,
            ValueType.ADDRESS: self._get_address_value,
            ValueType.TIMESTAMP: self._get_timestamp_value,
            ValueType.MESSAGE_ID: self._get_message_id_value,
            ValueType.BODY: self._get_body_value,
            ValueType.REFERENCES: self._get_reference_values,
        }
        return method_map[field.value_type](field)

    @handle_exceptions("Failed to get message value")
    def _get_message_value(self, field: MessageField) -> Any:
        return getattr(self.message, field.pypff_key, field.default)

    @handle_exceptions("Failed to get header value")
    def _get_header_value(self, field: MessageField) -> Any:
        return self.headers.get(field.pypff_key, field.default)

    @handle_exceptions("Failed to get address value")
    def _get_address_value(self, field: MessageField) -> Union[str, List[str], None]:
        value = self.headers.get(field.pypff_key)
        if value:
            value = re.sub(r"\r\n\s*", " ", value).lower()
            addresses = getaddresses([value])
            valid_addresses = [addr for _, addr in addresses if addr.strip()]
            return valid_addresses[0] if len(valid_addresses) == 1 else valid_addresses
        return field.default

    @handle_exceptions("Failed to get timestamp value")
    def _get_timestamp_value(self, field: MessageField) -> Optional[datetime]:
        timestamp = getattr(self.message, f"get_{field.pypff_key}_as_integer")()
        return (
            datetime(1601, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=timestamp // 10)
            if timestamp
            else None
        )

    @handle_exceptions("Failed to get message id value")
    def _get_message_id_value(self, field: MessageField) -> Optional[str]:
        value = self.headers.get(field.pypff_key)
        return value.strip("<>") if value else field.default

    @handle_exceptions("Failed to get body value")
    def _get_body_value(self, field: MessageField) -> Optional[str]:
        body = getattr(self.message, field.pypff_key)
        if body:
            try:
                return body.decode("utf-8")
            except UnicodeDecodeError:
                encoding = chardet.detect(body)["encoding"]
                return body.decode(encoding, errors="replace") if encoding else None
        return field.default

    @handle_exceptions("Failed to get references value")
    def _get_reference_values(self, field: MessageField) -> Optional[List[str]]:
        value = self.headers.get(field.pypff_key)
        values = re.split(r"[ ,]+", value.strip()) if value else []
        return [v.strip("<>") for v in values] if values else field.default

    def get(self, key: str) -> Any:
        return self.parsed_data.get(key)
