from pyspark.sql import SparkSession


# As we have a requirement to process gigabytes or terabytes of data fast
# I believe PySpark is the best tool to handle large datasets quickly and efficiently
# Initialize the Spark session
def get_spark_session(log_level='WARN'):
    spark_session = SparkSession.builder.master('local[1]'
                                                ).appName("CSVProcessor"
                                                          ).config('spark.driver.port', '4041').getOrCreate()
    spark_session.sparkContext.setLogLevel(log_level)

    return spark_session
