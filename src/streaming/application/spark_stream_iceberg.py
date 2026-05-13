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
    """Job streaming Kafka → Iceberg."""

    def execute(self, processed_df: DataFrame) -> None:
        sql = ConfigLoader.get_sql_config()
        out = sql.output
        temp_view = sql.kafka.temp_view or sql.job.name

        # Đăng ký UDF (optional, dùng được trong sql_conditions)
        TransformPhoneNumberUDF().register(self.spark)

        checkpoint = PathBuilder.build_checkpoint_path("main")
        target_table = PathBuilder.build_iceberg_table(out.target_table or sql.job.name)

        logger.info(
            f"📌 Job config:\n"
            f"  - target_table : {target_table}\n"
            f"  - checkpoint   : {checkpoint}\n"
            f"  - temp_view    : {temp_view}\n"
            f"  - write_mode   : {out.write_mode}\n"
            f"  - sql_condition: {bool(out.sql_conditions)}\n"
            f"  - columns      : {out.columns or 'ALL'}"
        )

        def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
            self.iceberg_connector.transform_and_write_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                target_table=target_table,
                sql_query=out.sql_conditions,
                temp_view_name=temp_view,
                selected_columns=out.columns or None,
                write_mode=out.write_mode,
            )

        query = (
            processed_df.writeStream
            .foreachBatch(_process_batch)
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime=sql.spark.trigger_interval)
            .queryName(f"streaming_{sql.job.name}")
            .start()
        )

        logger.info(f"✅ Streaming query started: id={query.id}, runId={query.runId}")
        query.awaitTermination()


def main() -> None:
    cli = CommandLineReader()

    try:
        ConfigLoader.initialize(
            config_file_path=cli.get_file_config_path(),
            sql_file_path=cli.get_file_sql_path(),
            prefix_phone_file_path=cli.get_file_prefix_phone_path(),
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