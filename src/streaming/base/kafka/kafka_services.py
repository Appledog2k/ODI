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
    def parse_cdc_stream(df: DataFrame, cdc_schema: StructType) -> DataFrame:
        return (
            df.select(
                F.from_json(F.col("value").cast("string"), cdc_schema).alias("data"),
                # Giữ Kafka metadata để dedup exact duplicate
                F.col("topic").alias("_kafka_topic"),
                F.col("partition").alias("_kafka_partition"),
                F.col("offset").alias("_kafka_offset"),
            )
            .select("data.*", "_kafka_topic", "_kafka_partition", "_kafka_offset")
        )

    @staticmethod
    def normalize_cdc(df: DataFrame) -> DataFrame:
        """
        Lấy payload đúng theo op type (c/u/r → after, d → before).
        Flatten payload + thêm metadata columns.
        """
        return (
            df.select(
                F.when(F.col("op") == "d", F.col("before"))
                .otherwise(F.col("after")).alias("payload"),
                F.col("op"),
                F.col("ts_ms"),
                F.col("source.ts_ms").alias("source_ts_ms"),
                F.col("_kafka_offset"),
            )
            .select(
                "payload.*",
                "op",
                "ts_ms",
                "source_ts_ms",
                "_kafka_offset",
            )
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("is_deleted", F.col("op") == F.lit("d"))
        )
    
    def dedup_latest_by_max_struct(
        df: DataFrame,
        pk_cols: List[str],
        order_cols: List[str],
    ) -> DataFrame:
        original_cols = df.columns
        
        sort_struct = F.struct(
            *[F.col(c).alias(f"_sort_{i}") for i, c in enumerate(order_cols)],
            F.struct(*[F.col(c) for c in original_cols]).alias("_data")
        )
        
        return (
            df.withColumn("_sort_struct", sort_struct)
            .groupBy(*pk_cols)
            .agg(F.max("_sort_struct").alias("_latest"))
            .select("_latest._data.*")
        )

    @staticmethod
    def process_kafka_data(raw_df: DataFrame) -> DataFrame:

        cdc_schema = StructType([
            StructField("before", json_structure), # chỗ  này cần hàm gen ra structure
            StructField("after", json_structure),
            StructField("op", StringType()),
            StructField("ts_ms", LongType())
        ])
        # parsed
        parsed_df = raw_df.select(
            from_json(col("value").cast("string"), cdc_schema).alias("data"),
        ).select("data.*")

        # Phân chia dữ liêu
        normalized_df = parsed_df.select(
            when(col("op") == "d", col("before")).otherwise(col("after")).alias("payload"),
            col("op"),
            col("ts_ms")
        ).select("payload.*", "op", "ts_ms").withColumn("ingest_date", current_timestamp())

        def upsert_to_iceberg(batch_df, batch_id):
            batch_df.createOrReplaceTempView("updates")
            
            spark.sql("""
                MERGE INTO bronze.db.table t
                USING updates s
                ON t.ID = s.ID
                WHEN MATCHED AND s.is_deleted = true THEN DELETE
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED AND s.is_deleted = false THEN INSERT *
            """)

        dedup_df.writeStream \
            .foreachBatch(upsert_to_iceberg) \
            .option("checkpointLocation", "s3a://bucket/checkpoint") \
            .trigger(processingTime="30 seconds") \
            .start()

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
