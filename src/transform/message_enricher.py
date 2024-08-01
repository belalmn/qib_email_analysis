import logging
import re
from typing import Optional

import langid

from src.transform.message_parser import ParsedMessage
from src.utils.exception_handler import handle_exceptions


class EnrichedMessage(ParsedMessage):
    first_in_thread: Optional[bool]
    previous_message_id: Optional[str]
    domain: Optional[str]
    language: Optional[str]
    spam_score: Optional[float]
    from_internal_domain: Optional[bool]
    subject_prefix: Optional[str]


class MessageEnricher:
    @staticmethod
    @handle_exceptions("Failed to enrich message")
    def enrich_message(parsed_message: ParsedMessage) -> EnrichedMessage:
        enriched_message = EnrichedMessage(
            **parsed_message.model_dump(),
            first_in_thread=MessageEnricher._is_first_in_thread(parsed_message),
            previous_message_id=MessageEnricher._get_previous_message_id(parsed_message),
            domain=MessageEnricher._get_domain(parsed_message),
            language=MessageEnricher._get_language(parsed_message),
            spam_score=MessageEnricher._get_spam_score(parsed_message),
            from_internal_domain=MessageEnricher._is_from_internal_domain(parsed_message),
            subject_prefix=MessageEnricher._get_subject_prefix(parsed_message),
        )
        return enriched_message

    @staticmethod
    def _is_first_in_thread(parsed_message: ParsedMessage) -> Optional[bool]:
        if parsed_message.in_reply_to is None:
            return True
        return False

    @staticmethod
    def _get_previous_message_id(parsed_message: ParsedMessage) -> Optional[str]:
        return parsed_message.in_reply_to

    @staticmethod
    def _get_domain(parsed_message: ParsedMessage) -> Optional[str]:
        if parsed_message.from_address:
            return parsed_message.from_address.split("@")[-1]
        return None

    @staticmethod
    def _get_language(parsed_message: ParsedMessage) -> Optional[str]:
        if parsed_message.plain_text_body:
            return langid.classify(parsed_message.plain_text_body)[0]
        return None

    @staticmethod
    def _get_spam_score(parsed_message: ParsedMessage) -> Optional[float]:
        return None

    @staticmethod
    def _is_from_internal_domain(parsed_message: ParsedMessage) -> Optional[bool]:
        if parsed_message.from_address:
            return parsed_message.from_address.endswith("qib.com.qa")
        return None

    @staticmethod
    def _get_subject_prefix(parsed_message: ParsedMessage) -> Optional[str]:
        pattern = r"(?i)((re|fwd?|fw):\s*|\[\w+\]\s*)"
        if parsed_message.subject:
            match = re.match(pattern, parsed_message.subject.strip())
            if match:
                return match.group()
        return None
