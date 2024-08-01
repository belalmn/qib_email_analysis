import logging
from typing import Any, List, Optional, Union

from sqlalchemy.orm import Session

from src.database.database import Database
from src.database.models import (
    Address,
    Base,
    Folder,
    Message,
    Recipient,
    RecipientType,
    References,
)
from src.transform.message_enricher import EnrichedMessage


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
            if not message.folder_name:
                print(f"------------- Folder Null")
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
                previous_message_id=message.previous_message_id,
                domain=message.domain,
                language=message.language,
                spam_score=message.spam_score,
                from_internal_domain=message.from_internal_domain,
                subject_prefix=message.subject_prefix,
                content_type=message.content_type,
            )
            session.add(db_message)

            self._add_recipients(session, db_message, message.to_address, RecipientType.TO)
            self._add_recipients(session, db_message, message.cc_address, RecipientType.CC)
            self._add_recipients(session, db_message, message.bcc_address, RecipientType.BCC)

            self._add_references(session, db_message, message.references)

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
            session.add(References(message=db_message, global_message_id=reference, order=order))
