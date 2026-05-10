import logging
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession

from streaming.utils.path_builder import PathBuilder
from streaming.utils.string_constants import StringConstants as SC

logger = logging.getLogger(__name__)


class IcebergConnector:
    """Connector ghi vào Iceberg table."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # =========================================================
    def transform_and_write_batch(
        self,
        batch_df: DataFrame,
        batch_id: int,
        target_table: str = "",
        sql_query: Optional[str] = None,
        temp_view_name: Optional[str] = None,
        selected_columns: Optional[List[str]] = None,
        write_mode: str = SC.APPEND,
    ) -> None:
        """Callback dùng cho writeStream.foreachBatch."""
        if batch_df.rdd.isEmpty():
            logger.info(f"[batch={batch_id}] Empty batch, skip.")
            return

        # 1) Apply SQL transform nếu có
        if sql_query and sql_query.strip():
            view = temp_view_name or f"_tmp_batch_{batch_id}"
            batch_df.createOrReplaceTempView(view)
            transformed = self.spark.sql(sql_query)
        else:
            transformed = batch_df

        # 2) Select columns nếu có
        if selected_columns:
            existing = [c for c in selected_columns if c in transformed.columns]
            if existing:
                transformed = transformed.select(*existing)

        # 3) Ghi Iceberg
        full_table = PathBuilder.build_iceberg_table(target_table)
        (
            transformed.write
            .format(SC.ICEBERG_FORMAT)
            .mode(write_mode)
            .save(full_table)
        )

        try:
            count = transformed.count()
        except Exception:  # noqa
            count = -1
        logger.info(
            f"[batch={batch_id}] ✅ Wrote {count} rows → {full_table} (mode={write_mode})"
        )