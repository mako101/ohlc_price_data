import os
import sys
import logging
from datetime import datetime
from tempfile import NamedTemporaryFile
from fastapi import UploadFile, HTTPException, Query
from fastapi.responses import JSONResponse

from pyspark.sql.functions import col
from sqlalchemy import insert
import csv_processor.utils as u
from csv_processor.app import CSVProcessor
from csv_processor.models import Position
from csv_processor.schema import expected_schema


# Set up basic logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
logging.basicConfig(level=getattr(logging, LOG_LEVEL),
                    format="%(asctime)s %(levelname)s - %(name)s: %(message)s",
                    stream=sys.stdout
                    )


if os.getenv('ENV') == 'DEV':
    from csv_processor.db_sqlite import get_db_session
else:
    from csv_processor.db_mysql import get_db_session


# Initialise the application
app = CSVProcessor()


@app.post("/data/")
def upload_csv(file: UploadFile):
    # Check if the uploaded file is a CSV file
    if file.content_type != "text/csv":
        raise HTTPException(status_code=400,
                            detail=f"Only CSV files are allowed.")
    try:
        # Read the contents of the CSV file
        # as we might be receiving massive files
        # lets load them into memory in chunks to avoid filling up the RAM
        # below is 1GB chunk
        # we will save the uploaded data as a temporary file on the filesystem
        # before passing it to Pyspark
        with NamedTemporaryFile(mode='wb', delete=False) as tmp_file:
            while contents := file.file.read(1024 ** 3):
                tmp_file.write(contents)

        # Load the CSV data into a PySpark DataFrame
        df = app.spark_session.read.csv(tmp_file.name,
                            header=True,
                            inferSchema=True,
                            )

        # As we have strict validation requirements for the uploaded CSV data,
        # we will check whether the schema inferred from the CSV matches exactly what we expect
        # this will verify both field names and data types within
        if df.schema != expected_schema:
            raise HTTPException(status_code=422,
                                detail=f"Data Validation Failed - Schema Mismatch. "
                                       f"expected: {expected_schema}, got: {df.schema}"
                                )

        # convert epoch time stamp to datetime string
        df = df.withColumn('UNIX', u.convert_timestamp_udf(col('UNIX')))

        # rename Dataframe columns to match the Position model object attributes
        new_column_names = ['timestamp', 'symbol', 'open_price', 'highest', 'lowest', 'close_price']
        df = df.toDF(*new_column_names)
        df.show(truncate=False)

        # prepare the data for bulk insertion with SQLAlchemy
        # convert Dataframe into a list of dictionaries
        # convert timestamp string into a datetime object
        df_rows = [row.asDict() for row in df.collect()]
        for row_dict in df_rows:
            row_dict['timestamp'] = datetime.strptime(row_dict['timestamp'], u.TIMESTAMP_FORMAT)
        logging.debug(df_rows)

        # Perform bulk insert into DB
        # this code does not validate whether the same records already exist in the database
        # As processing speed is crucial, adding checking for duplicates at this stage
        # would significantly degrade performance when adding new records
        with get_db_session() as session:
            session.execute(
                insert(Position),
                df_rows
            )
            session.commit()

        return JSONResponse(content={
            'result': 'success',
            'detail': f'Processed {len(df_rows)} entries from {file.filename}'},
            status_code=200)

    except Exception as e:
        logging.exception('Unhandled Error')
        if isinstance(e, HTTPException):
            raise
        else:
            raise HTTPException(status_code=500, detail=f"Unexpected Error: {str(e)}")


@app.get("/data")
def get_records(
        symbol: str = Query(default='BTCUSDT', description="Crypto pair symbol, eg `BTCUSDT`"),
        start_date: str = Query(default='2022-02-13 02:30:00',
                                description="Start date to query position data, in the format YYYY-MM-DD hh:mm:ss"),
        end_date: str = Query(default='2022-02-13 02:36:00',
                              description="End date to query position data, in the format YYYY-MM-DD hh:mm:ss"),
        limit: int = Query(default=5, description="Amount of results per page to return"),
        page: int = Query(default=1, description="Page to read the data from")
):
    try:

        with get_db_session() as session:
            query = session.query(Position).filter(
                Position.symbol == u.validate_symbol(symbol),
                Position.timestamp >= u.validate_date(start_date),
                Position.timestamp <= u.validate_date(end_date)
            ).order_by(Position.timestamp.desc())

            total_records = query.count()
            total_pages = int(total_records // limit + (1 if total_records % limit > 0 else 0))

            records = query.limit(limit).offset((page - 1) * limit).all()
            for record in records:
                logging.debug(record)

        if not records:
            return {
                "data": {'result': f'No records found for symbol {symbol} from {start_date} to {end_date}'}
            }

        return {
            "data": [record.__dict__ for record in records],
            "pagination": {
                "count": total_records,
                "page": page,
                "limit": limit,
                "pages": total_pages
            },
        }
    except Exception as e:
        logging.exception('Unhandled Error')
        if isinstance(e, HTTPException):
            raise
        else:
            raise HTTPException(status_code=500, detail=f"Unexpected Error: {str(e)}")
