import os
import sys
import requests
import logging
import pymysql
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from csv_processor.models import Base, Stock
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s - %(name)s: %(message)s",
                    stream=sys.stdout
                    )

# Load AlphaVantage API key

try:
    api_key = os.environ['API_KEY']
    mysql_db = os.environ['MYSQL_DB']
except KeyError:
    raise EnvironmentError('Please set `API_KEY` and `MYSQL_DB` environmental variables')


pymysql.install_as_MySQLdb()

# Define the SQLAlchemy engine and session
engine = create_engine(mysql_db)
Session = sessionmaker(bind=engine)
session = Session()

# Create the 'financial_data' table if it doesn't exist
Base.metadata.create_all(engine)

symbols = ['AAPL', 'IBM']
outputsize = 'compact'
date_format = "%Y-%m-%d"

for symbol in symbols:
    # Retrieve daily stock data from AlphaVantage
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize={outputsize}&apikey={api_key}'

    try:
        response = requests.get(url)
        data = response.json()['Time Series (Daily)']
    except:
        logging.exception(f'Failed to retrieve data from API: {response.text}')
        sys.exit(1)

    # Store the retrieved data in the database
    # Check for existing records before saving
    for date_string, values in data.items():
        try:
            date_object = datetime.strptime(date_string, date_format).date()
            if datetime.now().date() - date_object < timedelta(days=15):
                existing_record = session.query(Stock).filter_by(symbol=symbol, date=date_object).first()
                if existing_record:
                    logging.debug(f'Found exiting record for {symbol} on {date_string}, skipping')

                else:
                    stock = Stock(
                        symbol=symbol,
                        date=datetime.strptime(date_string, date_format).date(),
                        open_price=float(values['1. open']),
                        close_price=float(values['4. close']),
                        volume=float(values['6. volume'])
                    )
                    session.add(stock)
        except:
            logging.exception(f'ERROR: failed to process {values}')

session.commit()
session.close()
