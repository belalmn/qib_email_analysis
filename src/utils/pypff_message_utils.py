import email
import logging
from typing import Any, Dict, Iterator, List

import pypff
from pydantic import BaseModel, Field


class PypffMessage:
    def __init__(self, message: pypff.message):
        self.message: pypff.message = message
        self.headers: Dict[str, Any] = self._get_headers()

    _message_key_map: Dict[str, str] = {
        "identifier": "identifier",
        "subject": "conversation_topic",
        "sender_name": "sender_name",
        "headers": "transport_headers",
        "creation_time": "creation_time",
        "submit_time": "client_submit_time",
        "delivery_time": "delivery_time",
        "body": "plain_text_body",
    }

    _header_key_map: Dict[str, str] = {
        "from_address": "From",
        "to_address": "To",
        "cc_address": "CC",
        "bcc_address": "BCC",
    }

    _default_email_values: Dict[str, Any] = {
        "identifier": None,
        "subject": "",
        "sender_name": "",
        "headers": "",
        "from_address": None,
        "to_address": None,
        "cc_address": None,
        "bcc_address": None,
        "creation_time": None,
        "submit_time": None,
        "delivery_time": None,
        "body": "",
    }

    def get_value(self, key: str) -> Any:
        if key in self._header_key_map:
            return self._get_header_value(key)
        elif key in self._message_key_map:
            return self._get_message_value(key)
        else:
            logging.warning(f"Key {key} is not a valid message or header key")
            return None

    def _get_headers(self) -> Dict[str, Any]:
        headers = self.message.transport_headers or ""
        return dict(email.message_from_string(headers).items())

    def _get_header_value(self, key: str) -> str:
        default_value = self._default_email_values.get(key)
        return self.headers.get(self._header_key_map[key], default_value)

    def _get_message_value(self, key: str) -> Any:
        try:
            return getattr(self.message, self._message_key_map[key])
        except Exception as e:
            logging.error(f"Error getting value for key {key}: {e}")
            return self._default_email_values.get(key)
