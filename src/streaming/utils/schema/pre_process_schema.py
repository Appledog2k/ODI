from typing import Dict, List

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, FloatType, BooleanType, TimestampType, DateType,
)

from streaming.utils.string_constants import StringConstants as SC

_TYPE_MAP = {
    "string": StringType(),
    "str": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    "double": DoubleType(),
    "float": FloatType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "timestamp": TimestampType(),
    "date": DateType(),
}


class PreProcessSchema:
    def __init__(self):
        raise NotImplementedError("Utility class")

    @staticmethod
    def hashmap_schema(json_structure: List[Dict[str, str]]) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for item in json_structure:
            field_name = item.get(SC.FIELD)
            field_type = item.get(SC.TYPE, "string")
            if field_name:
                result[field_name] = field_type
        return result

    @staticmethod
    def generate_struct_type(json_structure: List[Dict[str, str]]) -> StructType:
        fields = []
        for item in json_structure:
            name = item.get(SC.FIELD)
            type_str = str(item.get(SC.TYPE, "string")).lower().strip()
            spark_type = _TYPE_MAP.get(type_str, StringType())
            fields.append(StructField(name, spark_type, nullable=True))
        return StructType(fields)
