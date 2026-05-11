import logging
from abc import abstractmethod

from pyspark.sql import DataFrame

from streaming.base.kafka.kafka_services import KafkaServices
from streaming.base.spark.base_application import BaseApplication
from streaming.connector.iceberg_connector import IcebergConnector
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

        self.iceberg_connector = IcebergConnector(self.spark)

        self.raw_kafka_df = KafkaServices.create_session_connect_kafka(self.spark)

        self.processed_df = KafkaServices.process_kafka_data(self.raw_kafka_df)
        logger.info(
            f"Kafka stream processed. Schema columns: {self.processed_df.columns}"
        )

        self.execute(self.processed_df)

    @abstractmethod
    def execute(self, processed_df: DataFrame) -> None:
        ...
