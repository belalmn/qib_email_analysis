import logging
from typing import Any, List, Optional, Union

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from tqdm import tqdm

from src.database.database import Database
from src.database.db_models import (
    Address,
    Base,
    Classification,
    Domain,
    Entity,
    Message,
    Product,
    Reference,
    Summary,
    Topic,
    WordFrequency,
)


class DataLoader:
    def __init__(self, database: Database):
        self.database = database

    def clear_all_data(self) -> None:
        self.database.drop_all_tables()

    def create_tables(self) -> None:
        Base.metadata.drop_all(bind=self.database.engine)
        logging.info("All tables have been dropped from the database.")
        Base.metadata.create_all(bind=self.database.engine)
        logging.info("All tables have been created in the database.")

    def load_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        """Loads a Pandas DataFrame into the specified SQL table."""
        if not hasattr(self, f"_load_{table_name}"):
            logging.error(f"Table name '{table_name}' does not have a corresponding load method.")
            return

        load_method = getattr(self, f"_load_{table_name}")
        self.database.execute_in_session(lambda session: load_method(session, df))

    def _load_messages(self, session: Session, df: pd.DataFrame) -> None:
        for _, row in tqdm(df.iterrows()):
            try:
                topic = self._get_or_create(session, Topic, topic_id=row["topic_id"])
                message = Message(
                    message_id=row["message_id"],
                    topic_id=topic.topic_id,
                    is_spam=row["is_spam"],
                    subject=row["subject"],
                    subject_prefix=row["subject_prefix"],
                    submit_time=row["submit_time"],
                    delivery_time=row["delivery_time"],
                    html_body=row["html_body"],
                    plain_text_body=row["plain_text_body"],
                    from_name=row["from_name"],
                    previous_message_id=row["previous_message_id"],
                    first_in_thread=row["first_in_thread"],
                    num_previous_messages=row["num_previous_messages"],
                    thread_id=row["thread_id"],
                    sender_domain=row["sender_domain"],
                    is_internal=row["is_internal"],
                    clean_text=row["clean_text"],
                    response_time=row["response_time"],
                    language=row["language"],
                )
                session.add(message)
            except SQLAlchemyError as e:
                logging.error(f"Failed to insert message: {e}")
                session.rollback()
                continue

    def _load_topics(self, session: Session, df: pd.DataFrame) -> None:
        for _, row in df.iterrows():
            try:
                topic = Topic(
                    topic_id=row["topic_id"],
                    topic_description=row["topic_description"],
                )
                session.add(topic)
            except SQLAlchemyError as e:
                logging.error(f"Failed to insert topic: {e}")
                session.rollback()
                continue

    def _load_addresses(self, session: Session, df: pd.DataFrame) -> None:
        for _, row in df.iterrows():
            try:
                message = self._get_or_create(session, Message, message_id=row["message_id"])

                address = Address(
                    message_id=message.message_id,
                    address_type=row["address_type"],
                    address=row["address"],
                )
                session.add(address)
            except SQLAlchemyError as e:
                logging.error(f"Failed to insert address: {e}")
                session.rollback()
                continue

    def _load_references(self, session: Session, df: pd.DataFrame) -> None:
        for _, row in df.iterrows():
            try:
                message = self._get_or_create(session, Message, message_id=row["message_id"])

                reference = Reference(
                    message_id=message.message_id,
                    reference_message_id=row["reference_message_id"],
                )
                session.add(reference)
            except SQLAlchemyError as e:
                logging.error(f"Failed to insert reference: {e}")
                session.rollback()
                continue

    def _load_domains(self, session: Session, df: pd.DataFrame) -> None:
        for _, row in df.iterrows():
            try:
                message = self._get_or_create(session, Message, message_id=row["message_id"])

                domain = Domain(
                    message_id=message.message_id,
                    domain=row["domain"],
                )
                session.add(domain)
            except SQLAlchemyError as e:
                logging.error(f"Failed to insert domain: {e}")
                session.rollback()
                continue

    def _load_classifications(self, session: Session, df: pd.DataFrame) -> None:
        for _, row in df.iterrows():
            try:
                message = self._get_or_create(session, Message, message_id=row["message_id"])

                classification = Classification(
                    message_id=message.message_id,
                    category=row["category"],
                )
                session.add(classification)
            except SQLAlchemyError as e:
                logging.error(f"Failed to insert classification: {e}")
                session.rollback()
                continue

    def _load_products(self, session: Session, df: pd.DataFrame) -> None:
        for _, row in df.iterrows():
            try:
                message = self._get_or_create(session, Message, message_id=row["message_id"])

                product = Product(
                    message_id=message.message_id,
                    product=row["product"],
                )
                session.add(product)
            except SQLAlchemyError as e:
                logging.error(f"Failed to insert product: {e}")
                session.rollback()
                continue

    def _load_entities(self, session: Session, df: pd.DataFrame) -> None:
        for _, row in df.iterrows():
            try:
                message = self._get_or_create(session, Message, message_id=row["message_id"])

                entity = Entity(
                    message_id=message.message_id,
                    entity_type=row["entity_type"],
                    entity_value=row["entity_value"],
                )
                session.add(entity)
            except SQLAlchemyError as e:
                logging.error(f"Failed to insert entity: {e}")
                session.rollback()
                continue

    def _load_summaries(self, session: Session, df: pd.DataFrame) -> None:
        for _, row in df.iterrows():
            try:
                message = self._get_or_create(session, Message, message_id=row["message_id"])

                summary = Summary(
                    message_id=message.message_id,
                    summary=row["summary"],
                )
                session.add(summary)
            except SQLAlchemyError as e:
                logging.error(f"Failed to insert summary: {e}")
                session.rollback()
                continue

    def _load_word_frequencies(self, session: Session, df: pd.DataFrame) -> None:
        for _, row in df.iterrows():
            try:
                topic = self._get_or_create(session, Topic, topic_id=row["topic_id"])

                word_frequency = WordFrequency(
                    topic_id=topic.topic_id,
                    word=row["word"],
                    frequency=row["frequency"],
                )
                session.add(word_frequency)
            except SQLAlchemyError as e:
                logging.error(f"Failed to insert word frequency: {e}")
                session.rollback()
                continue

    def _get_or_create(self, session: Session, model: type, **kwargs: Any) -> Any:
        instance = session.query(model).filter_by(**kwargs).first()
        if instance:
            return instance
        else:
            instance = model(**kwargs)
            session.add(instance)
            session.flush()
            return instance
