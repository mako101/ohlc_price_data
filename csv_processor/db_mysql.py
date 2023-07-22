import os

import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from csv_processor.models import Base

def get_db_session():
    # Check MYSQL root password is set
    try:
        mysql_pwd = os.environ['MYSQL_PWD']
    except KeyError:
        raise EnvironmentError('Please set `MYSQL_PWD` environmental variable')

    mysql_db = f'mysql://root:{mysql_pwd}@mysql:3306/mydb'

    pymysql.install_as_MySQLdb()

    # Define the SQLAlchemy engine and session
    engine = create_engine(mysql_db, pool_size=5, pool_recycle=3600)

    # Create the 'positions' table if it doesn't exist
    Base.metadata.create_all(engine)

    session = sessionmaker(bind=engine)
    return session()
