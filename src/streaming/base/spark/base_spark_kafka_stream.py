import logging
from abc import abstractmethod

from pyspark.sql import DataFrame

from streaming.base.kafka.kafka_services import KafkaServices
from streaming.base.spark.base_application import BaseApplication
from streaming.connector.iceberg.iceberg_connector import IcebergConnector
from streaming.connector.kafka.kafka_stream_connector import KafkaStreamConnector
from streaming.utils.model.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class BaseSparkKafkaStream(BaseApplication):
    def __init__(self, app_name: str):
        super().__init__(app_name)
        self.iceberg_connector: IcebergConnector = None
        self.raw_kafka_df: DataFrame = None
        self.processed_df: DataFrame = None

    def init(self, sc) -> None:
        sql = ConfigLoader.get_sql_config()
        logger.info(f"Init streaming job: {sql.job.name}")

        # Read raw Kafka stream
        self.raw_kafka_df = KafkaStreamConnector.create_stream_kafka(self.spark)
        logger.info(f"Connected to Kafka topic: {sql.kafka.topics_in}")

        # Parse Kafka data theo CDC format (schema_cdc) + extract schema_data
        schema_cdc = sql.kafka.schema_cdc
        schema_data = sql.kafka.schema_data

        self.processed_df = KafkaServices.parse_value_kafka(
            self.raw_kafka_df,
            schema_cdc,
            schema_data
        )

        logger.info(
            f"✓ Kafka stream processed\n"
            f"  - CDC structure: {[f.name for f in schema_cdc.fields]}\n"
            f"  - Extracted columns: {self.processed_df.columns}"
        )

        # Execute pipeline (to be implemented by subclasses)
        self.execute(self.processed_df)

    @abstractmethod
    def execute(self, processed_df: DataFrame) -> None:
        ...
