import os
import pymysql
import contextlib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from csv_processor.models import Base


# Check MYSQL root password is set
try:
    mysql_pwd = os.environ['MYSQL_PWD']
except KeyError:
    raise EnvironmentError('Please set `MYSQL_PWD` environmental variable')

mysql_db = f'mysql://root:{mysql_pwd}@mysql:3306/mydb'


pymysql.install_as_MySQLdb()

# Define the SQLAlchemy engine and session
engine = create_engine(mysql_db, pool_size=5, pool_recycle=3600)
Session = sessionmaker(bind=engine)
# session = Session()
