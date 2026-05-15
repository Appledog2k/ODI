import logging

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class KafkaServices:
    def __init__(self):
        raise NotImplementedError("Utility class")

    @staticmethod
    def parse_value_kafka(df: DataFrame, schema: StructType) -> DataFrame:
        pass
