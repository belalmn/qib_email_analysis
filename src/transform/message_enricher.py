import re
from typing import List, Optional, Union

import langid
import pypff
from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)

from src.extract.message_parser import ParsedMessage


class EnrichedMessage(ParsedMessage):
    first_in_thread: Optional[bool]
    previous_message_id: Optional[str]
    domain: Optional[str]
    language: Optional[str]
    spam_score: Optional[float]
    from_internal_domain: Optional[bool]
    subject_prefix: Optional[str]


class MessageEnricher:
    def __init__(self, parsed_message: ParsedMessage):
        self.parsed_message = parsed_message

    def enrich(self) -> EnrichedMessage:
        self.enriched_message = EnrichedMessage(
            **self.parsed_message.model_dump(),
            first_in_thread=self._is_first_in_thread(),
            previous_message_id=self._get_previous_message_id(),
            domain=self._get_domain(),
            language=self._get_language(),
            spam_score=self._get_spam_score(),
            from_internal_domain=self._is_from_internal_domain(),
            subject_prefix=self._get_subject_prefix(),
        )
        return self.enriched_message

    def _is_first_in_thread(self) -> Optional[bool]:
        if self.parsed_message.in_reply_to is None:
            return True
        return False

    def _get_previous_message_id(self) -> Optional[str]:
        return self.parsed_message.in_reply_to

    def _get_domain(self) -> Optional[str]:
        if self.parsed_message.from_address:
            return self.parsed_message.from_address.split("@")[-1]
        return None

    def _get_language(self) -> Optional[str]:
        if self.parsed_message.body:
            return langid.classify(self.parsed_message.body)[0]
        return None

    def _get_spam_score(self) -> Optional[float]:
        return None

    def _is_from_internal_domain(self) -> Optional[bool]:
        if self.parsed_message.from_address:
            return self.parsed_message.from_address.endswith("qib.com.qa")
        return None

    def _get_subject_prefix(self) -> Optional[str]:
        return None
