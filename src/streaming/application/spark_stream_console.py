import logging

from pyspark.sql import DataFrame
from streaming.utils.logging import setup_logging

from streaming.base.spark.base_spark_kafka_stream import BaseSparkKafkaStream
from streaming.utils.command_line_reader import CommandLineReader
from streaming.utils.exception_handler import handle_fatal_error
from streaming.utils.model.config_loader import ConfigLoader
from streaming.utils.path_builder import PathBuilder

logger = logging.getLogger(__name__)


class SparkStreamConsole(BaseSparkKafkaStream):
    """
    Console streaming: Read từ Kafka → Parse dữ liệu → Display tới console.
    Dữ liệu được parse theo schema_data từ config.
    """

    def execute(self, processed_df: DataFrame) -> None:
        """
        Execute streaming pipeline.

        Args:
            processed_df: DataFrame đã được parse từ Kafka messages
                         (chứa các column từ schema_data)
        """
        sql = ConfigLoader.get_sql_config()
        temp_view = sql.kafka.temp_view or sql.job.name
        checkpoint = PathBuilder.build_checkpoint_path("console")

        def _process_batch(batch_df: DataFrame, batch_id: int) -> None:
            """Process mỗi batch của Kafka stream."""
            if batch_df.rdd.isEmpty():
                logger.info(f"[batch={batch_id}] Empty, skip.")
                return

            # Nếu có SQL conditions, apply SQL trên batch data
            if sql.output.sql_conditions and sql.output.sql_conditions.strip():
                batch_df.createOrReplaceTempView(temp_view)
                result = self.spark.sql(sql.output.sql_conditions)
                logger.info(f"[batch={batch_id}] Applied SQL conditions")
            else:
                result = batch_df

            count = result.count()
            logger.info(f"[batch={batch_id}] {count} rows")

            # Display dữ liệu đã parse
            logger.info(f"[batch={batch_id}] Parsed data from Kafka:")
            result.show(truncate=False, n=20)

        # Start streaming query
        query = (
            processed_df.writeStream
            .foreachBatch(_process_batch)
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime=sql.spark.trigger_interval)
            .queryName(f"console_{sql.job.name}")
            .start()
        )

        logger.info(
            f"✅ Console streaming started\n"
            f"  - Job: {sql.job.name}\n"
            f"  - Topic: {sql.kafka.topics_in}\n"
            f"  - Query ID: {query.id}\n"
            f"  - Checkpoint: {checkpoint}"
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

    app = SparkStreamConsole(app_name=f"console-{sql.job.name}")
    app.start()


if __name__ == "__main__":
    main()
