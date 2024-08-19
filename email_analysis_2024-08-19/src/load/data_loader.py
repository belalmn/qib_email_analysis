import logging
from typing import Any, List, Optional, Union

from sqlalchemy.orm import Session

from src.database.database import Database
from src.database.db_models import (
    Address,
    Base,
    Folder,
    Message,
    Recipient,
    RecipientType,
    Reference,
    SubMessage,
)
from src.transform.message_enricher import EnrichedMessage
from src.transform.sub_message_parser import SubMessage as SubMessageObject


class DataLoader:
    def __init__(self, database: Database):
        self.database = database

    def clear_all_data(self) -> None:
        self.database.drop_all_tables()

    def create_tables(self) -> None:
        Base.metadata.create_all(bind=self.database.engine)
        logging.info("All tables have been created in the database.")

    def load(self, message: EnrichedMessage) -> None:
        def _load(session: Session) -> None:
            folder = self._get_or_create(session, Folder, name=message.folder_name)
            from_address = self._get_or_create(session, Address, email=message.from_address)

            db_message = Message(
                provider_email_id=message.provider_email_id,
                global_message_id=message.global_message_id,
                subject=message.subject,
                sender_name=message.sender_name,
                creation_time=message.creation_time,
                submit_time=message.submit_time,
                delivery_time=message.delivery_time,
                folder=folder,
                from_address=from_address,
                plain_text_body=message.plain_text_body,
                rich_text_body=message.rich_text_body,
                html_body=message.html_body,
                first_in_thread=message.first_in_thread,
                num_emails_in_thread=message.num_emails_in_thread,
                previous_message_id=message.in_reply_to,
                domain=message.domain,
                language=message.language,
                spam_score=message.spam_score,
                from_internal_domain=message.from_internal_domain,
                subject_prefix=message.subject_prefix,
                content_type=message.content_type,
                classification=message.classification,
            )
            session.add(db_message)

            self._add_recipients(session, db_message, message.to_address, RecipientType.TO)
            self._add_recipients(session, db_message, message.cc_address, RecipientType.CC)
            self._add_recipients(session, db_message, message.bcc_address, RecipientType.BCC)

            self._add_references(session, db_message, message.references)

            self._add_sub_messages(session, db_message, message.sub_messages)

        self.database.execute_in_session(_load)

    def _get_or_create(self, session: Session, model: type, **kwargs: Any) -> Any:
        instance = session.query(model).filter_by(**kwargs).first()
        if instance:
            return instance
        else:
            instance = model(**kwargs)
            session.add(instance)
            session.flush()
            return instance

    def _add_recipients(
        self,
        session: Session,
        db_message: Message,
        addresses: Optional[Union[str, List[str]]],
        recipient_type: RecipientType,
    ) -> None:
        if not addresses:
            return

        if isinstance(addresses, str):
            addresses = [addresses]

        for address in addresses:
            db_address = self._get_or_create(session, Address, email=address)
            db_message.recipients.append(Recipient(address=db_address, type=recipient_type))

    def _add_references(
        self, session: Session, db_message: Message, references: Optional[List[str]]
    ) -> None:
        if not references:
            return

        for order, reference in enumerate(references):
            session.add(Reference(message=db_message, global_message_id=reference, order=order))

    def _add_sub_messages(
        self, session: Session, db_message: Message, sub_messages: Optional[List[SubMessageObject]]
    ) -> None:
        if not sub_messages:
            return

        print(len(sub_messages))
        for sub_message in sub_messages:
            session.add(
                SubMessage(
                    message=db_message,
                    subject=sub_message.subject,
                    sender_name=sub_message.sender_name,
                    from_address=sub_message.from_address,
                    submit_time=sub_message.submit_time,
                    receiver_name=sub_message.receiver_name,
                    to_address=sub_message.to_address,
                    message_body=sub_message.plain_body_text,
                    global_message_id=sub_message.global_message_id,
                )
            )
