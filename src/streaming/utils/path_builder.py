from streaming.utils.model.config_loader import ConfigLoader
from streaming.utils.string_constants import StringConstants as SC


class PathBuilder:
    def __init__(self):
        raise NotImplementedError("Utility class — do not instantiate")

    @staticmethod
    def build_checkpoint_path(stream_name: str = "main") -> str:
        common = ConfigLoader.get_common_config()
        sql = ConfigLoader.get_sql_config()

        if sql.output.checkpoint_location:
            return sql.output.checkpoint_location

        bucket = common.ceph.warehouse_bucket
        job_name = sql.job.name
        return f"s3a://{bucket}/checkpoints/{job_name}/{stream_name}"

    @staticmethod
    def build_iceberg_table(target: str = "", default_namespace: str = "bronze") -> str:
        sql = ConfigLoader.get_sql_config()
        target = target or sql.output.target_table or sql.job.name

        parts = target.split(".")
        if len(parts) == 3:
            return target
        if len(parts) == 2:
            return f"{SC.ICEBERG_CATALOG}.{target}"
        return f"{SC.ICEBERG_CATALOG}.{default_namespace}.{target}"

    @staticmethod
    def build_warehouse_path() -> str:
        """Path warehouse cho Iceberg catalog."""
        common = ConfigLoader.get_common_config()
        return f"s3a://{common.ceph.warehouse_bucket}/warehouse"
