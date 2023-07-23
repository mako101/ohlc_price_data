# SQLite3 Backend for local testing
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker

from csv_processor.models import Base


def get_db_session():
    logging.debug('Using SQLite3 DB Backend')
    conn_string = 'sqlite:///../test.sqlite3'
    engine = create_engine(conn_string)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)
    return session()
