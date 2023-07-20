from fastapi import FastAPI, UploadFile, HTTPException
from pyspark.sql import SparkSession
from tempfile import NamedTemporaryFile
from csv_processor.schema import expected_schema

app = FastAPI()

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
        df.show()

        # As we have strict validation requirements for the uploaded CSV data,
        # we will check whether the schema inferred from the CSV matches exactly what we expect
        # this will verify both field names and data types within
        if df.schema != expected_schema:
            raise HTTPException(status_code=422,
                                detail=f"Data Validation Failed - Schema Mismatch. "
                                       f"expected: {expected_schema}, got: {df.schema}"
                                )

        return {'result': 'success', 'detail': f'Processed {file.filename} successfully'}

    except Exception as e:
        print(e)
        if isinstance(e, HTTPException):
            raise
        else:
            raise HTTPException(status_code=500, detail=f"Unexpected Error: {str(e)}")
