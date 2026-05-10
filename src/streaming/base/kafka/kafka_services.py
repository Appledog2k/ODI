import logging
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType

from streaming.utils.string_constants import StringConstants as SC
from streaming.utils.model.config_loader import ConfigLoader
from streaming.utils.schema.pre_process_schema import PreProcessSchema

logger = logging.getLogger(__name__)


class KafkaServices:

    def __init__(self):
        raise NotImplementedError("Utility class")

    # =========================================================
    @staticmethod
    def create_session_connect_kafka(spark: SparkSession) -> DataFrame:
        common = ConfigLoader.get_common_config()
        sql = ConfigLoader.get_sql_config()
        k_common = common.kafka
        k_job = sql.kafka

        options: Dict[str, str] = {
            SC.BOOTSTRAP_SERVERS: k_common.bootstrap_servers,
            SC.SUBSCRIBE: k_job.topics_in,
            SC.STARTING_OFFSETS: k_job.auto_offset_reset,
            SC.KAFKA_MAX_OFFSET_PER_TRIGGER: str(k_job.max_offsets_per_trigger),
            SC.FAIL_ON_DATA_LOSS: SC.FALSE,
            SC.KAFKA_KEY_DESERIALIZER: SC.STRING_DESERIALIZER,
            SC.KAFKA_VALUE_DESERIALIZER: SC.STRING_DESERIALIZER,
        }

        if k_common.secure:
            jaas = (
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                f'username="{k_common.user}" password="{k_common.password}";'
            )
            options.update({
                SC.SECURITY_PROTOCOL: k_common.security_protocol,
                SC.SASL_MECHANISM: k_common.sasl_mechanism,
                SC.KAFKA_SASL_JAAS_CONFIG: jaas,
            })

        df = spark.readStream.format(SC.KAFKA).options(**options).load()
        logger.info(
            f"Kafka stream created: topic={k_job.topics_in}, "
            f"offsets={k_job.auto_offset_reset}, "
            f"maxOffsets/trigger={k_job.max_offsets_per_trigger}"
        )
        return df

    @staticmethod
    def process_kafka_data(raw_df: DataFrame) -> DataFrame:
        sql = ConfigLoader.get_sql_config()
        k_job = sql.kafka

        if not k_job.json_structure:
            return raw_df.selectExpr(
                "CAST(key AS STRING) AS _kafka_key",
                "CAST(value AS STRING) AS _kafka_value",
                "topic AS _kafka_topic",
                "partition AS _kafka_partition",
                "offset AS _kafka_offset",
                "timestamp AS _kafka_timestamp",
            )

        schema: StructType = PreProcessSchema.generate_struct_type(k_job.json_structure)

        parsed = (
            raw_df
            .selectExpr(
                "CAST(value AS STRING) AS json_str",
                "topic AS _kafka_topic",
                "partition AS _kafka_partition",
                "offset AS _kafka_offset",
                "timestamp AS _kafka_timestamp",
            )
            .withColumn(SC.FIELD, from_json(col("json_str"), schema))
            .select(
                f"{SC.FIELD}.*",
                "_kafka_topic",
                "_kafka_partition",
                "_kafka_offset",
                "_kafka_timestamp",
            )
        )
        return parsed
