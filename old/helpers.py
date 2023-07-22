from datetime import datetime, timedelta


def get_previous_date(days_ago: int):
    previous_date = datetime.now().date() - timedelta(days=days_ago)
    return previous_date.strftime("%Y-%m-%d")


def validate_date(date_string):
    try:
        datetime.strptime(date_string, "%Y-%m-%d").date()
        return date_string
    except:
        raise ValueError(f'Date {date_string} does not match excepted format: YYYY-MM-DD')


def validate_symbol(symbol_string):
    if (not symbol_string.isalpha()) or len(symbol_string) > 10:
        raise ValueError(f'{symbol_string} is not a valid symbol (should be a short string containing only letters)')

    return symbol_string.upper()

