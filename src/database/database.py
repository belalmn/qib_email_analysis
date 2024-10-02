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

    @staticmethod
    def from_credentials(
        username: str, password: str, host: str, database: str, port: int = 3306
    ) -> "Database":
        """
        Create a Database instance from a set of connection credentials.

        Args:
            username (str): Username to use for the connection.
            password (str): Password to use for the connection.
            host (str): Hostname or IP address of the database.
            database (str): Name of the database to connect to.
            port (int): TCP port number to connect to. Defaults to 3306.

        Returns:
            Database: A new Database instance with the specified connection settings.
        """
        db_url = URL.create(
            drivername="mysql+pymysql",
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )
        return Database(db_url)

    @contextmanager
    def session(self):
        """
        Provides a context manager to manage a database session.

        This context manager ensures that any exceptions raised while using the session are
        properly rolled back and logged before being re-raised. Additionally, the session is
        always closed when the context manager is exited.

        Yields:
            sqlalchemy.orm.Session: A valid database session.
        """
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
        """
        Execute a function within a session context.

        Args:
            func: The function to execute.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            The return value of the function, if any.
        """
        with self.session() as session:
            return func(session, *args, **kwargs)

    def drop_all_tables(self) -> None:
        """
        Drop all tables in the database.

        This method will drop all tables in the database, destroying any data that
        they contain. Use with caution.

        Returns:
            None
        """
        self.metadata.reflect(bind=self.engine)
        self.metadata.drop_all(self.engine)
        logging.info("All tables have been dropped from the database.")
