import json
import logging
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType

from streaming.utils.model.sql_config import SchemaConfig

logger = logging.getLogger(__name__)


class KafkaServices:
    """Kafka services cho streaming data processing."""

    def __init__(self):
        raise NotImplementedError("Utility class")

    @staticmethod
    def parse_value_kafka(df: DataFrame, schema_cdc: SchemaConfig, schema_data: SchemaConfig) -> DataFrame:
        """
        Parse Kafka DataFrame với CDC format.

        Quy trình:
        1. Parse Kafka value (JSON string) theo schema_cdc → được: before, after, source, op, ts_ms
        2. Dựa vào `op`:
           - Nếu `op == "d"` (DELETE): extract từ field 'before'
           - Nếu `op != "d"` (CREATE/UPDATE): extract từ field 'after'
        3. Cast sang đúng type theo schema_data
        4. Thêm cột `op` vào kết quả

        Args:
            df: Spark DataFrame từ Kafka (chứa column 'value' là JSON string CDC format)
            schema_cdc: SchemaConfig định nghĩa structure của CDC record
                       (fields: before, after, source, op, ts_ms)
            schema_data: SchemaConfig định nghĩa structure của data trong field 'after'/'before'

        Returns:
            DataFrame với các column từ schema_data + thêm cột 'op'

        Example:
            Kafka value (CREATE):
            {
              "before": null,
              "after": {"systemname": "SAP", "oltptype": "OLTP", "nametable": "sys1"},
              "op": "c",
              ...
            }
            Result: [systemname, oltptype, nametable, op] = ["SAP", "OLTP", "sys1", "c"]

            Kafka value (DELETE):
            {
              "before": {"systemname": "SAP", "oltptype": "OLTP", "nametable": "sys1"},
              "after": null,
              "op": "d",
              ...
            }
            Result: [systemname, oltptype, nametable, op] = ["SAP", "OLTP", "sys1", "d"]
        """
        # Tạo UDF để extract dữ liệu từ 'before' hoặc 'after' field dựa vào op
        def extract_cdc_fields(json_str: str) -> Dict[str, Any]:
            """
            Extract fields từ CDC record theo operation type.
            - DELETE (op='d'): lấy từ 'before'
            - CREATE/UPDATE: lấy từ 'after'
            """
            try:
                cdc_record = json.loads(json_str)
                op = cdc_record.get("op", "")

                # Chọn source data dựa vào operation
                if op == "d":  # DELETE
                    source_data = cdc_record.get("before")
                    logger.debug(f"DELETE operation: using 'before' data")
                else:  # CREATE, UPDATE, TRUNCATE
                    source_data = cdc_record.get("after")
                    logger.debug(f"{op.upper()} operation: using 'after' data")

                if source_data is None:
                    logger.debug(f"Source data is None for operation '{op}'")
                    result = {f.name: None for f in schema_data.fields}
                else:
                    result = {}
                    for field in schema_data.fields:
                        value = source_data.get(field.name)
                        try:
                            result[field.name] = KafkaServices._cast_value(value, field.type)
                        except Exception as e:
                            logger.warning(
                                f"Failed to cast '{field.name}' to '{field.type}': {e}"
                            )
                            result[field.name] = value

                # Thêm operation type vào result
                result["op"] = op

                return result

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in Kafka value: {e}")
                result = {f.name: None for f in schema_data.fields}
                result["op"] = None
                return result
            except Exception as e:
                logger.error(f"Error extracting CDC fields: {e}")
                result = {f.name: None for f in schema_data.fields}
                result["op"] = None
                return result

        # Build StructType từ schema_data + thêm 'op' field
        struct_type = KafkaServices._build_struct_type_with_op(schema_data)

        # Tạo UDF
        extract_udf = F.udf(extract_cdc_fields, returnType=struct_type)

        # Apply UDF
        parsed_df = df.select(extract_udf(F.col("value")).alias("parsed_data"))

        # Explode struct columns ra thành individual columns
        result_df = parsed_df.select("parsed_data.*")

        row_count = result_df.count()
        logger.info(
            f"✓ Parsed {row_count} CDC messages from Kafka\n"
            f"  - Schema: {[f.name for f in schema_data.fields] + ['op']}"
        )

        return result_df

    @staticmethod
    def _build_struct_type(schema: SchemaConfig) -> StructType:
        """Build PySpark StructType từ SchemaConfig."""
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, LongType,
            DoubleType, FloatType, BooleanType, TimestampType, DateType
        )

        type_map = {
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
            "struct": StringType(),  # Giữ nguyên JSON string
        }

        fields = []
        for field in schema.fields:
            spark_type = type_map.get(str(field.type).lower().strip(), StringType())
            fields.append(StructField(field.name, spark_type, nullable=True))

        return StructType(fields)

    @staticmethod
    def _build_struct_type_with_op(schema: SchemaConfig) -> StructType:
        """Build PySpark StructType từ SchemaConfig + thêm cột 'op' (operation type)."""
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, LongType,
            DoubleType, FloatType, BooleanType, TimestampType, DateType
        )

        type_map = {
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
            "struct": StringType(),  # Giữ nguyên JSON string
        }

        fields = []

        # Thêm các field từ schema
        for field in schema.fields:
            spark_type = type_map.get(str(field.type).lower().strip(), StringType())
            fields.append(StructField(field.name, spark_type, nullable=True))

        # Thêm field 'op' để lưu operation type (c, u, d, t)
        fields.append(StructField("op", StringType(), nullable=True))

        return StructType(fields)

    @staticmethod
    def _parse_json_to_dict(json_value: str, schema_data: SchemaConfig) -> Optional[Dict[str, Any]]:
        """
        Internal function: Parse Kafka JSON message theo CDC format.
        Non-UDF version cho use trong other functions.
        """
        try:
            # Parse JSON string thành dict
            cdc_record = json.loads(json_value)

            # Extract dữ liệu từ field 'after' (dữ liệu sau khi thay đổi)
            after_data = cdc_record.get("after")

            if after_data is None:
                logger.warning("Field 'after' không tìm thấy trong CDC record")
                return None

            # Extract các field từ schema_data
            result = {}
            for field_schema in schema_data.fields:
                field_name = field_schema.name
                field_type = field_schema.type

                # Lấy giá trị từ 'after' data
                value = after_data.get(field_name)

                # Cast giá trị sang type phù hợp
                try:
                    casted_value = KafkaServices._cast_value(value, field_type)
                    result[field_name] = casted_value
                except ValueError as e:
                    logger.warning(
                        f"Failed to cast field '{field_name}' to type '{field_type}': {e}"
                    )
                    result[field_name] = value  # Giữ nguyên giá trị nếu cast thất bại

            return result

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing Kafka message: {e}")
            return None

    @staticmethod
    def _cast_value(value: Any, target_type: str) -> Any:
        """
        Cast giá trị sang type được chỉ định.

        Args:
            value: Giá trị cần cast
            target_type: Type cần cast tới (string, int, long, double, boolean, etc.)

        Returns:
            Giá trị đã được cast

        Raises:
            ValueError: Nếu cast thất bại
        """
        if value is None:
            return None

        target_type = str(target_type).lower().strip()

        if target_type in ("string", "str"):
            return str(value)
        elif target_type in ("int", "integer"):
            return int(value)
        elif target_type in ("long", "bigint"):
            return int(value)
        elif target_type in ("double", "float"):
            return float(value)
        elif target_type in ("boolean", "bool"):
            if isinstance(value, bool):
                return value
            return str(value).lower() in ("true", "1", "yes")
        elif target_type == "struct":
            # Nếu là struct, trả về dict
            if isinstance(value, dict):
                return value
            elif isinstance(value, str):
                return json.loads(value)
            return value
        else:
            # Default: convert to string
            return str(value)

    @staticmethod
    def get_operation(json_value: str) -> Optional[str]:
        """
        Lấy operation type từ CDC record.

        Args:
            json_value: String JSON từ Kafka message

        Returns:
            Operation type: "c" (create), "u" (update), "d" (delete), "t" (truncate)
            Hoặc None nếu parse thất bại
        """
        try:
            cdc_record = json.loads(json_value)
            return cdc_record.get("op")
        except (json.JSONDecodeError, AttributeError) as e:
            logger.error(f"Failed to extract operation: {e}")
            return None

    @staticmethod
    def get_before_data(json_value: str) -> Optional[Dict[str, Any]]:
        """
        Lấy dữ liệu 'before' từ CDC record (dữ liệu trước thay đổi).

        Args:
            json_value: String JSON từ Kafka message

        Returns:
            Dict chứa dữ liệu 'before', hoặc None nếu không tìm thấy/parse thất bại
        """
        try:
            cdc_record = json.loads(json_value)
            return cdc_record.get("before")
        except (json.JSONDecodeError, AttributeError) as e:
            logger.error(f"Failed to extract 'before' data: {e}")
            return None

    @staticmethod
    def get_after_data(json_value: str) -> Optional[Dict[str, Any]]:
        """
        Lấy dữ liệu 'after' từ CDC record (dữ liệu sau thay đổi).

        Args:
            json_value: String JSON từ Kafka message

        Returns:
            Dict chứa dữ liệu 'after', hoặc None nếu không tìm thấy/parse thất bại
        """
        try:
            cdc_record = json.loads(json_value)
            return cdc_record.get("after")
        except (json.JSONDecodeError, AttributeError) as e:
            logger.error(f"Failed to extract 'after' data: {e}")
            return None

    @staticmethod
    def get_source_metadata(json_value: str) -> Optional[Dict[str, Any]]:
        """
        Lấy metadata 'source' từ CDC record.

        Args:
            json_value: String JSON từ Kafka message

        Returns:
            Dict chứa metadata 'source', hoặc None nếu không tìm thấy/parse thất bại
        """
        try:
            cdc_record = json.loads(json_value)
            return cdc_record.get("source")
        except (json.JSONDecodeError, AttributeError) as e:
            logger.error(f"Failed to extract source metadata: {e}")
            return None

    @staticmethod
    def filter_by_operation(df: DataFrame, operations: list) -> DataFrame:
        """
        Filter DataFrame dựa vào operation type (c/u/d/t).

        Args:
            df: Spark DataFrame từ Kafka (chứa column 'value')
            operations: List of operation types (e.g., ["c", "u"])

        Returns:
            Filtered DataFrame

        Example:
            >>> # Chỉ lấy INSERT và UPDATE
            >>> df_filtered = KafkaServices.filter_by_operation(kafka_df, ["c", "u"])
        """
        def get_op(json_str):
            try:
                record = json.loads(json_str)
                return record.get("op")
            except:
                return None

        get_op_udf = F.udf(get_op)

        if not operations:
            return df

        # Filter rows có operation type trong list
        filtered_df = df.filter(get_op_udf(F.col("value")).isin(operations))
        logger.info(f"Filtered to {filtered_df.count()} messages with operations: {operations}")

        return filtered_df

    @staticmethod
    def extract_cdc_fields(df: DataFrame) -> DataFrame:
        """
        Extract CDC fields (op, ts_ms, source.table, etc.) thành separate columns.

        Args:
            df: Spark DataFrame từ Kafka

        Returns:
            DataFrame với thêm các columns: op, ts_ms, source_table, source_db

        Example:
            >>> df_with_cdc = KafkaServices.extract_cdc_fields(kafka_df)
        """
        def extract_op(json_str):
            try:
                return json.loads(json_str).get("op")
            except:
                return None

        def extract_ts_ms(json_str):
            try:
                return json.loads(json_str).get("ts_ms")
            except:
                return None

        def extract_source_table(json_str):
            try:
                source = json.loads(json_str).get("source", {})
                return source.get("table")
            except:
                return None

        def extract_source_db(json_str):
            try:
                source = json.loads(json_str).get("source", {})
                return source.get("db")
            except:
                return None

        op_udf = F.udf(extract_op)
        ts_ms_udf = F.udf(extract_ts_ms)
        table_udf = F.udf(extract_source_table)
        db_udf = F.udf(extract_source_db)

        result_df = (
            df
            .withColumn("op", op_udf(F.col("value")))
            .withColumn("ts_ms", ts_ms_udf(F.col("value")))
            .withColumn("source_table", table_udf(F.col("value")))
            .withColumn("source_db", db_udf(F.col("value")))
        )

        logger.info("Extracted CDC fields: op, ts_ms, source_table, source_db")
        return result_df
