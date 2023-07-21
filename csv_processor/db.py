import os
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from csv_processor.models import Base, Position


# Check MYSQL root password is set
try:
    mysql_pwd = os.environ['MYSQL_PWD']
except KeyError:
    raise EnvironmentError('Please set `MYSQL_PWD` environmental variable')

mysql_db = f'mysql://root:{mysql_pwd}@mysql:3306/mydb'


pymysql.install_as_MySQLdb()

# Define the SQLAlchemy engine and session
engine = create_engine(mysql_db)
Session = sessionmaker(bind=engine)
session = Session()

# Create the table if it doesn't exist
Base.metadata.create_all(engine)

