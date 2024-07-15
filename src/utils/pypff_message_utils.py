import email
import logging
import re
from datetime import datetime, timedelta, timezone
from email.utils import getaddresses
from enum import Enum
from typing import Any, Dict, List, Union

import pypff
from pydantic import BaseModel


class ValueType(Enum):
    GENERIC_MESSAGE_VALUE = "message"
    GENERIC_HEADER_VALUE = "general_header"
    ADDRESS = "address_header"
    TIMESTAMP = "timestamp"
    MESSAGE_ID = "message_id"


class ValueInfo(BaseModel):
    pypff_key: str
    value_type: ValueType = ValueType.GENERIC_MESSAGE_VALUE
    default_value: Any = None


class PypffMessage:
    def __init__(self, message: pypff.message):
        self.message: pypff.message = message
        self.headers: Dict[str, Any] = self._headers_from_message()

    def _headers_from_message(self) -> Dict[str, Any]:
        headers = self.message.transport_headers or ""
        return dict(email.message_from_string(headers).items())

    def _generic_value_from_message(self, value_info: ValueInfo) -> Any:
        try:
            return getattr(self.message, value_info.pypff_key)
        except Exception as e:
            logging.error(f"Error getting value for key {value_info.pypff_key}: {e}")
            return value_info.default_value

    def _generic_value_from_header(self, value_info: ValueInfo) -> Any:
        return self.headers.get(value_info.pypff_key, value_info.default_value)

    def _address_from_header(self, value_info: ValueInfo) -> Any:
        header_value = self.headers.get(value_info.pypff_key, value_info.default_value)
        if header_value is not None:
            header_value = re.sub(r"\r\n\s*", " ", header_value).lower()
            addresses = getaddresses([header_value])
            valid_addresses = []

            for name, address in addresses:
                if address.strip():
                    valid_addresses.append(address.strip())

            if not valid_addresses:
                return value_info.default_value
            elif len(valid_addresses) == 1:
                return valid_addresses[0]
            else:
                return valid_addresses

        return value_info.default_value

    def _timestamp_from_message(self, value_info: ValueInfo) -> Any:
        match value_info.pypff_key:
            case "creation_time":
                timestamp = self.message.get_creation_time_as_integer()
            case "client_submit_time":
                timestamp = self.message.get_client_submit_time_as_integer()
            case "delivery_time":
                timestamp = self.message.get_delivery_time_as_integer()
            case _:
                logging.error(f"Invalid timestamp key {value_info.pypff_key}")
                return value_info.default_value
        return self._integer_time_as_datetime(timestamp)

    def _message_id_from_header(self, value_info: ValueInfo) -> Any:
        message_id = self.headers.get(value_info.pypff_key, value_info.default_value)
        if message_id:
            return message_id.strip("<>")
        return value_info.default_value

    _handler_map = {
        ValueType.GENERIC_MESSAGE_VALUE: _generic_value_from_message,
        ValueType.GENERIC_HEADER_VALUE: _generic_value_from_header,
        ValueType.ADDRESS: _address_from_header,
        ValueType.TIMESTAMP: _timestamp_from_message,
        ValueType.MESSAGE_ID: _message_id_from_header,
    }

    _value_map: Dict[str, ValueInfo] = {
        "identifier": ValueInfo(pypff_key="identifier"),
        "subject": ValueInfo(pypff_key="conversation_topic"),
        "sender_name": ValueInfo(pypff_key="sender_name"),
        "headers": ValueInfo(pypff_key="transport_headers"),
        "creation_time": ValueInfo(pypff_key="creation_time", value_type=ValueType.TIMESTAMP),
        "submit_time": ValueInfo(pypff_key="client_submit_time", value_type=ValueType.TIMESTAMP),
        "delivery_time": ValueInfo(pypff_key="delivery_time", value_type=ValueType.TIMESTAMP),
        "body": ValueInfo(pypff_key="plain_text_body"),
        "from_address": ValueInfo(pypff_key="From", value_type=ValueType.ADDRESS),
        "to_address": ValueInfo(pypff_key="To", value_type=ValueType.ADDRESS),
        "cc_address": ValueInfo(pypff_key="CC", value_type=ValueType.ADDRESS),
        "bcc_address": ValueInfo(pypff_key="BCC", value_type=ValueType.ADDRESS),
        "in_reply_to": ValueInfo(pypff_key="In-Reply-To", value_type=ValueType.GENERIC_HEADER_VALUE),
        "message_id": ValueInfo(pypff_key="Message-ID", value_type=ValueType.MESSAGE_ID),
    }

    def get_value(self, key: str) -> Any:
        if key in self._value_map:
            value_info = self._value_map[key]
            handler = self._handler_map[value_info.value_type]
            return handler(self, value_info)
        else:
            logging.warning(f"Key {key} is not a valid message or header key")
            return None

    @staticmethod
    def _integer_time_as_datetime(filetime: int) -> Union[datetime, None]:
        # Pypff integer time follows the FILETIME format, which is 100ns intervals since 1601-01-01
        if filetime:
            return datetime(1601, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=filetime // 10)
        return None
