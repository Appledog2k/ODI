from typing import Any

from pyspark.sql import DataFrame, SparkSession

from streaming.utils.model.config_loader import ConfigLoader
from streaming.utils.string_constants import StringConstants as sc


class KafkaStreamConnector:
    def __init__(self):
        raise NotImplementedError("KafkaStreamConnector is a utility class and should not be instantiated.")

    @staticmethod
    def create_stream_kafka(spark: SparkSession) -> DataFrame:
        options = KafkaStreamConnector._build_options()
        df = spark.readStream.format(sc.KAFKA).option(**options).load()
        return df

    @staticmethod
    def _build_options() -> dict[str, Any]:
        common = ConfigLoader.get_common_config()
        sql = ConfigLoader.get_sql_config()
        k_common = common.kafka
        k_job = sql.kafka

        options: dict[str, str] = {
            sc.BOOTSTRAP_SERVERS: k_common.bootstrap_servers,
            sc.SUBSCRIBE: k_job.topics_in,
            sc.STARTING_OFFSETS: k_job.auto_offset_reset,
            sc.KAFKA_MAX_OFFSET_PER_TRIGGER: str(k_job.max_offsets_per_trigger),
            sc.FAIL_ON_DATA_LOSS: sc.FALSE,
            sc.KAFKA_KEY_DESERIALIZER: sc.STRING_DESERIALIZER,
            sc.KAFKA_VALUE_DESERIALIZER: sc.STRING_DESERIALIZER,
        }

        if k_common.secure:
            options.update(KafkaStreamConnector._build_secure_options(k_common))

        return options

    @staticmethod
    def _build_secure_options(k_common) -> dict[str, str]:
        jaas = (
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{k_common.user}" password="{k_common.password}";'
        )
        return {
            sc.SECURITY_PROTOCOL: k_common.security_protocol,
            sc.SASL_MECHANISM: k_common.sasl_mechanism,
            sc.KAFKA_SASL_JAAS_CONFIG: jaas,
        }
