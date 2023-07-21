import sys
import logging
from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from tempfile import NamedTemporaryFile
from csv_processor.schema import expected_schema
from csv_processor.utils import convert_timestamp_udf
from csv_processor.models import Position
# from csv_processor.db import session


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s - %(name)s: %(message)s",
                    stream=sys.stdout
                    )

app = FastAPI()


# As we have a requirement to process gigabytes or terabytes of data fast
# I believe PySpark is the best tool to handle large datasets quickly and efficiently
# Initialize the Spark session
spark = SparkSession.builder.master('local[1]').appName("CSVProcessor").getOrCreate()


@app.post("/upload/")
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
        df = spark.read.csv(tmp_file.name,
            header=True,
            inferSchema=True,
        )

        df.printSchema()
        df.show(truncate=False)

        # As we have strict validation requirements for the uploaded CSV data,
        # we will check whether the schema inferred from the CSV matches exactly what we expect
        # this will verify both field names and data types within
        if df.schema != expected_schema:
            raise HTTPException(status_code=422,
                                detail=f"Data Validation Failed - Schema Mismatch. "
                                       f"expected: {expected_schema}, got: {df.schema}"
                                )

        timestamp_format = "%Y-%m-%d %H:%M:%S"
        df = df.withColumn('UNIX', convert_timestamp_udf(col('UNIX')))
        df.show(truncate=False)

        df_dict = [row.asDict() for row in df.collect()]
        print(df_dict)

        return JSONResponse(content={
            'result': 'success',
            'detail': f'Processed {file.filename} successfully'},
            status_code=200)

    except Exception as e:
        print(e)
        if isinstance(e, HTTPException):
            raise
        else:
            raise HTTPException(status_code=500, detail=f"Unexpected Error: {str(e)}")
