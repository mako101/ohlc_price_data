import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def convert_timestamp(timestamp: int) -> str:
    """
    Convert UNIX timestamp to UTC datetime string
    A custom converter function is required
    as we need to trim trailing zeros
    from the CSV timestamp before converting
    """
    timestamp_obj = datetime.datetime.utcfromtimestamp(
        int(timestamp)/1000)

    timestamp_string = timestamp_obj.strftime(TIMESTAMP_FORMAT)
    return timestamp_string


convert_timestamp_udf = udf(lambda x: convert_timestamp(x), StringType())
