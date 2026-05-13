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
            # java options
            .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/opt/data_temp_hadoop")
            .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/opt/data_temp_hadoop -XX:+ExitOnOutofMemoryError")
            # catalog config
            .config("spark.sql.extensions", SC.ICEBERG_EXTENSIONS)
            .config(f"spark.sql.catalog.{catalog}", SC.ICEBERG_SPARK_CATALOG)
            .config(f"spark.sql.catalog.{catalog}.type", "hive")
            .config(f"spark.sql.catalog.{catalog}.uri", "thrift://10.64.1.144:9083")
            .config(f"spark.sql.catalog.{catalog}.warehouse", "s3a://mcpolap/bronze/") # check lại phần build path
            .config("spark.sql.defaultCatalog", catalog)
            # cephs3
            .config(SC.S3A_ENDPOINT, ceph.ceph_endpoint)
            .config(SC.ACCESS_KEY, ceph.ceph_access_key)
            .config(SC.SECRET_KEY, ceph.ceph_secret_key)
            .config(SC.S3A_PATH_STYLE_ACCESS, SC.TRUE)
            .config(SC.S3A_CONNECTION_SSL_ENABLED, SC.FALSE)
            .config(SC.S3A_AWS_CREDENTIALS_PROVIDER, SC.S3A_SIMPLE_AWS)
            .config(SC.S3A_IMPL_DISABLE_CACHE, SC.TRUE)
            .config(SC.S3A_CONNECTION_TIMEOUT, "200000")
            .config(SC.S3A_CONNECTION_ESTABLISH_TIMEOUT, "5000")
            .config("spark.hadoop.fs.s3a.proxy.host", None)
            .config("spark.hadoop.fs.s3a.proxy.port", 0)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # tunning stream
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
            .config("spark.hadoop.fs.s3a.connection.maximum", "100")
            .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
            .config("spark.hadoop.fs.s3a.retry.limit", "10")
            .config("spark.hadoop.fs.s3a.retry.interval", "500ms")
            # tunning upload
            .config("spark.hadoop.fs.s3a.fast.upload", "true") # fast upload
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")  # ram buffer
            .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "4") # số block song song
            .config("spark.hadoop.fs.s3a.multipart.size", "32M") # kích thước part
            .config("spark.hadoop.fs.s3a.multipart.threshold", "32M")
            .config("spark.hadoop.fs.s3a.threads.max", "20")
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
            .config("spark.hadoop.fs.s3a.max.total.tasks", "20")
            # iceberg tunning
            .config("spark.sql.catalog.bronze.commit.retry.num-retries", "10")
            .config("spark.sql.catalog.bronze.commit.retry.min-wait-ms", "100")
            .config("spark.sql.catalog.bronze.commit.retry.max-wait-ms", "60000")
            .config("spark.sql.catalog.bronze.commit.retry.total-timeout-ms", "1800000")
            .config("spark.sql.catalog.bronze.write.target-file-size-bytes", "134217728")  # 128MB
            .config("spark.sql.catalog.bronze.write.distribution-mode", "hash")
            .config("spark.sql.catalog.bronze.write.fanout.enabled", SC.TRUE)
            # tunning spark
            .config("spark.local.dir", "/opt/data_temp_hadoop")
            .config("spark.shuffe.service.enabled", SC.TRUE)
        )

        self.spark = builder.enableHiveSupport().getOrCreate()
        self.spark.sparkContext.setLogLevel("INFO")
        logger.info(f"SparkSession initialized: app={self.app_name}")

    @abstractmethod
    def init(self, sc) -> None:
        ...

    def close(self) -> None:
        if self.spark is not None:
            try:
                self.spark.stop()
                logger.info("SparkSession stopped")
            except Exception as e:  # noqa
                logger.error(f"Error stopping SparkSession: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False