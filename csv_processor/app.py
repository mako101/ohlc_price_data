from fastapi import FastAPI
from typing import Any
from csv_processor.spark import get_spark_session


class CSVProcessor(FastAPI):
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._spark_session = None

    @property
    def spark_session(self):
        if not self._spark_session:
            self._spark_session = get_spark_session()
        return self._spark_session

