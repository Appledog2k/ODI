import logging
from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql import SparkSession

from streaming.utils.string_constants import StringConstants as SC
from streaming.utils.model.config_loader import ConfigLoader
from streaming.utils.path_builder import PathBuilder

logger = logging.getLogger(__name__)


class SparkSessionWrapper(ABC):
    def __init__(self, app_name: str):
        self.app_name = app_name
        self.spark: Optional[SparkSession] = None
        self._init_spark_session()

    # =========================================================
    def _init_spark_session(self) -> None:
        common = ConfigLoader.get_common_config()
        sql = ConfigLoader.get_sql_config()

        ceph = common.ceph
        ice = common.iceberg
        spark_tuning = sql.spark
        catalog = SC.ICEBERG_CATALOG
        warehouse = PathBuilder.build_warehouse_path()

        builder = (
            SparkSession.builder
            .appName(self.app_name)
            .config("spark.sql.extensions", SC.ICEBERG_EXTENSIONS)
            .config(f"spark.sql.catalog.{catalog}", SC.ICEBERG_SPARK_CATALOG)
            .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
            .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
            .config(f"spark.sql.catalog.{catalog}.io-impl", SC.ICEBERG_S3_FILE_IO)
            .config(SC.SPARK_HADOOP_FS_S3A_IMPL, SC.HADOOP_FS_S3A_FILE_SYSTEM)
            .config(SC.S3A_ENDPOINT, ceph.ceph_endpoint)
            .config(SC.ACCESS_KEY, ceph.ceph_access_key)
            .config(SC.SECRET_KEY, ceph.ceph_secret_key)
            .config(SC.S3A_PATH_STYLE_ACCESS, str(ceph.path_style_access).lower())
            .config(SC.S3A_CONNECTION_SSL_ENABLED, str(ceph.ssl_enabled).lower())
            .config(SC.S3A_AWS_CREDENTIALS_PROVIDER, SC.S3A_SIMPLE_AWS)
            .config(SC.S3A_IMPL_DISABLE_CACHE, SC.TRUE)
            .config(SC.S3A_ATTEMPTS_MAXIMUM, ceph.s3a_attempts_max)
            .config(SC.S3A_CONNECTION_TIMEOUT, ceph.s3a_connection_timeout)
            .config(SC.S3A_CONNECTION_ESTABLISH_TIMEOUT, "5000")
            .config(SC.SPARK_SQL_SHUFFLE_PARTITIONS, str(spark_tuning.shuffle_partitions))
            .config(SC.SPARK_DEFAULT_PARALLELISM, str(spark_tuning.default_parallelism))
            .config(SC.MIN_BATCHES_TO_RETAIN, str(spark_tuning.min_batches_to_retain))
            .config(SC.NO_DATA_PROGRESS_EVENT_INTERVAL,
                    str(spark_tuning.no_data_progress_event_interval))
            .config(SC.NO_DATA_MICRO_BATCHES,
                    str(spark_tuning.no_data_micro_batches_enabled).lower())
            .config(SC.SPARK_STREAMING_STOP_GRACE_FULLY_ON_SHUTDOWN, SC.TRUE)
            .config(SC.CLEANER_CHECKPOINTS, SC.TRUE)
            .config(SC.MAX_TO_STRING_FIELDS, "500")
        )

        for key, value in ice.raw.items():
            # Set ở cấp catalog default → áp dụng cho mọi table mới
            builder = builder.config(
                f"spark.sql.catalog.{catalog}.table-default.{key}",
                str(value),
            )

        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"✅ SparkSession initialized: app={self.app_name}, warehouse={warehouse}")

    @abstractmethod
    def init(self, sc) -> None:
        """Subclass override để định nghĩa logic khởi tạo nghiệp vụ."""
        ...

    def close(self) -> None:
        if self.spark is not None:
            try:
                self.spark.stop()
                logger.info("🛑 SparkSession stopped")
            except Exception as e:  # noqa
                logger.warning(f"Error stopping SparkSession: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False