from datetime import datetime

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def validate_date(date_string):
    try:
        datetime.strptime(date_string, TIMESTAMP_FORMAT).date()
        return date_string
    except:
        raise ValueError(f'Date {date_string} does not match excepted format: YYYY-MM-DD hh-mm-ss')


def validate_symbol(symbol_string):
    if (not symbol_string.isalpha()) or len(symbol_string) > 10:
        raise ValueError(f'{symbol_string} is not a valid symbol (should be a short string containing only letters)')

    return symbol_string.upper()


def convert_timestamp(timestamp: int) -> str:
    """
    Convert UNIX timestamp to UTC datetime string
    A custom converter function is required
    as we need to trim trailing zeros
    from the CSV timestamp before converting
    """
    timestamp_obj = datetime.utcfromtimestamp(
        int(timestamp) / 1000)

    timestamp_string = timestamp_obj.strftime(TIMESTAMP_FORMAT)
    return timestamp_string


convert_timestamp_udf = udf(lambda x: convert_timestamp(x), StringType())
