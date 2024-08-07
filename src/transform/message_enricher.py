import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Union

import html2text
import langid

# from langchain_community.llms.llamafile import Llamafile
from langchain_community.llms.ollama import Ollama

from src.transform.message_parser import ParsedMessage
from src.transform.sub_message_parser import SubMessage, split_message
from src.utils.exception_handler import handle_exceptions

htmlConverter = html2text.HTML2Text()
htmlConverter.ignore_links = True
htmlConverter.ignore_images = True
htmlConverter.ignore_emphasis = True

# llm = Llamafile()
llm = Ollama(model="gemma2:2b")


class EnrichedMessage(ParsedMessage):
    first_in_thread: Optional[bool]
    num_emails_in_thread: Optional[int]
    domain: Optional[str]
    language: Optional[str]
    spam_score: Optional[float]
    from_internal_domain: Optional[bool]
    subject_prefix: Optional[str]
    sub_messages: Optional[List[SubMessage]]
    classification: Optional[str]


class MessageEnricher:
    @staticmethod
    @handle_exceptions("Failed to enrich message")
    def enrich_message(parsed_message: ParsedMessage) -> EnrichedMessage:

        # Adding new values to enriched_message
        enriched_message = EnrichedMessage(
            **parsed_message.model_dump(),
            first_in_thread=MessageEnricher._is_first_in_thread(parsed_message),
            num_emails_in_thread=MessageEnricher._num_emails_in_thread(parsed_message),
            domain=MessageEnricher._get_domain(parsed_message),
            language=MessageEnricher._get_language(parsed_message),
            spam_score=MessageEnricher._get_spam_score(parsed_message),
            from_internal_domain=MessageEnricher._is_from_internal_domain(parsed_message),
            subject_prefix=MessageEnricher._get_subject_prefix(parsed_message),
            sub_messages=None,
            classification=None,
        )

        # Updating values of enriched_message
        enriched_message.plain_text_body = MessageEnricher._get_plain_body_value(enriched_message)
        enriched_message.sub_messages = MessageEnricher._split_messages(enriched_message)
        enriched_message.classification = MessageEnricher._get_classification(enriched_message)

        return enriched_message

    @staticmethod
    def _is_first_in_thread(parsed_message: ParsedMessage) -> Optional[bool]:
        if parsed_message.in_reply_to is None:
            return True
        return False

    @staticmethod
    def _num_emails_in_thread(parsed_message: ParsedMessage) -> Optional[int]:
        if parsed_message.in_reply_to is None:
            return 1
        else:
            return len(parsed_message.references) if parsed_message.references else None

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
    def _split_messages(enriched_message: EnrichedMessage) -> Optional[List[SubMessage]]:
        if (
            enriched_message.plain_text_body
            and enriched_message.references
            and len(enriched_message.references) > 1
        ):
            submessages = split_message(
                enriched_message.plain_text_body,
                enriched_message.references + [enriched_message.global_message_id],
            )
            return submessages
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

    @staticmethod
    def _get_plain_body_value(enriched_message: EnrichedMessage) -> Optional[str]:
        if enriched_message.html_body and not enriched_message.plain_text_body:
            return htmlConverter.handle(enriched_message.html_body)
        elif enriched_message.plain_text_body:
            return enriched_message.plain_text_body
        return None

    @staticmethod
    def _get_classification(enriched_message: EnrichedMessage) -> Optional[str]:
        if enriched_message.plain_text_body:
            result = llm.invoke(
                f"""Based on the following email text, produce a classification of the email as one of the following:
                - 'customer_inquiry'
                - 'customer_complaint'
                - 'customer_feedback'
                - 'account_issue'
                - 'loan_application'
                - 'fraud_report'
                - 'technical_support'
                - 'product_information'
                - 'marketing_opt_out'
                - 'internal_communication'
                - 'partner_inquiry'
                - 'vendor_communication'
                - 'job_application'
                - 'press_inquiry'
                - 'regulatory_correspondence'
                - 'phishing_attempt'
                - 'spam'
                - 'service_request'
                - 'branch_information'
                - 'event_rsvp'
                                
                Do not provide any other text in the response other than the classification. Your answer should be one of the above classifications.

                Here are some examples, with the expected classification. Respond similarly.
                                
                Email text: 'I am writing to inquire about the status of my loan application.'
                Classification: loan_application
                                
                Email text: 'I am writing to complain about the poor service I received.'
                Classification: customer_complaint
                                
                Email text: 'I am writing to provide feedback on your product.'
                Classification: customer_feedback

                Email text: {enriched_message.plain_text_body}
                Classification: 
            """
            )
            print(result)
        return None
