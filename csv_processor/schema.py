from pyspark.sql.types import StructType, StructField, StringType,  LongType, DoubleType

expected_schema = StructType([
    StructField("UNIX", LongType(), True),
    StructField("SYMBOL", StringType(), True),
    StructField("OPEN", DoubleType(), True),
    StructField("HIGH", DoubleType(), True),
    StructField("LOW", DoubleType(), True),
    StructField("CLOSE", DoubleType(), True)
])