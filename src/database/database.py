import logging
from contextlib import contextmanager
from typing import Any, Callable, List, TypeVar, Union

from sqlalchemy import MetaData, create_engine, inspect
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

T = TypeVar("T")


class Database:
    def __init__(self, db_url: Union[str, URL]):
        self.engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.metadata = MetaData()

    @contextmanager
    def session(self):
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"Database error: {str(e)}")
            raise
        finally:
            session.close()

    def execute_in_session(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        with self.session() as session:
            return func(session, *args, **kwargs)

    def drop_all_tables(self) -> None:
        self.metadata.reflect(bind=self.engine)
        self.metadata.drop_all(self.engine)
        logging.info("All tables have been dropped from the database.")

    @staticmethod
    def from_credentials(
        username: str, password: str, host: str, database: str, port: int = 3306
    ) -> "Database":
        db_url = URL.create(
            drivername="mysql+pymysql",
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )
        return Database(db_url)
