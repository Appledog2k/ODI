import logging

from pyspark.sql import DataFrame

from streaming.base.spark.base_spark_kafka_stream import BaseSparkKafkaStream
from streaming.utils.command_line_reader import CommandLineReader
from streaming.utils.exception_handler import handle_fatal_error
from streaming.utils.logging_setup import setup_logging
from streaming.utils.model.config_loader import ConfigLoader
from streaming.utils.path_builder import PathBuilder
from streaming.utils.udf.transform_phone_udf import TransformPhoneNumberUDF

logger = logging.getLogger(__name__)


class SparkStreamIceberg(BaseSparkKafkaStream):
    """
    Streaming job: Kafka → Parse dữ liệu → Apply SQL → Iceberg.

    Pipeline:
    1. Read từ Kafka topic (topic_in)
    2. Parse dữ liệu theo schema_data từ config (by BaseSparkKafkaStream)
    3. Apply SQL condition nếu có (output.sql_conditions)
    4. Select columns nếu cần (output.columns)
    5. Write đến Iceberg table (target_table)
    """

    def execute(self, processed_df: DataFrame) -> None:
        """
        Execute Iceberg streaming pipeline.

        Args:
            processed_df: DataFrame đã được parse từ Kafka (schema từ schema_data)
        """
        sql = ConfigLoader.get_sql_config()
        out = sql.output
        temp_view = sql.kafka.temp_view or sql.job.name

        # Đăng ký UDF (optional, dùng được trong sql_conditions)
        TransformPhoneNumberUDF().register(self.spark)

        checkpoint = PathBuilder.build_checkpoint_path("main")
        target_table = PathBuilder.build_iceberg_table(out.target_table or sql.job.name)

        logger.info(
            f"📌 Job config:\n"
            f"  - Input topic  : {sql.kafka.topics_in}\n"
            f"  - target_table : {target_table}\n"
            f"  - checkpoint   : {checkpoint}\n"
            f"  - temp_view    : {temp_view}\n"
            f"  - write_mode   : {out.write_mode}\n"
            f"  - sql_condition: {bool(out.sql_conditions)}\n"
            f"  - columns      : {out.columns or 'ALL'}"
        )

        def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
            """Process mỗi batch từ Kafka stream."""
            if batch_df.rdd.isEmpty():
                logger.info(f"[batch={batch_id}] Empty batch, skip.")
                return

            logger.info(f"[batch={batch_id}] Processing {batch_df.count()} rows from Kafka")

            # Gọi iceberg connector để transform + write
            self.iceberg_connector.transform_and_write_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                target_table=target_table,
                sql_query=out.sql_conditions,
                temp_view_name=temp_view,
                selected_columns=out.columns or None,
                write_mode=out.write_mode,
            )

        # Start streaming query
        query = (
            processed_df.writeStream
            .foreachBatch(_process_batch)
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime=sql.spark.trigger_interval)
            .queryName(f"streaming_{sql.job.name}")
            .start()
        )

        logger.info(
            f"✅ Iceberg streaming started\n"
            f"  - Query ID: {query.id}\n"
            f"  - Run ID: {query.runId}\n"
            f"  - Target table: {target_table}"
        )

        # Wait for termination
        query.awaitTermination()


def main() -> None:
    cli = CommandLineReader()

    try:
        ConfigLoader.initialize(
            config_file_path=cli.get_file_config_path(),
            sql_file_path=cli.get_file_sql_path(),
            properties_file_path=cli.get_properties_file_path()
        )
    except Exception as e:
        handle_fatal_error("Failed to load config", e)

    sql = ConfigLoader.get_sql_config()
    setup_logging(job_name=sql.job.name)

    app_name = f"streaming-{sql.job.name}"
    app = SparkStreamIceberg(app_name=app_name)
    app.start()

if __name__ == "__main__":
    main()