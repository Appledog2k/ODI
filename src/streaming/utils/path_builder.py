from streaming.utils.model.config_loader import ConfigLoader
from streaming.utils.string_constants import StringConstants as sc


class PathBuilder:
    def __init__(self):
        raise NotImplementedError("Utility class — do not instantiate")

    @staticmethod
    def build_checkpoint_path(stream_name: str = "main") -> str:
        common = ConfigLoader.get_common_config()
        sql = ConfigLoader.get_sql_config()

        return f"s3a://{common.ceph.bucket_checkpoints}/checkpoints/{sql.job.name}/{stream_name}"

    @staticmethod
    def build_iceberg_table(target: str = "", default_namespace: str = "bronze") -> str:
        sql = ConfigLoader.get_sql_config()
        target = target or sql.output.target_table or sql.job.name

        parts = target.split(".")
        if len(parts) == 3:
            return target
        if len(parts) == 2:
            return f"{sc.ICEBERG_CATALOG}.{target}"
        return f"{sc.ICEBERG_CATALOG}.{default_namespace}.{target}"

    @staticmethod
    def build_lakehouse_path() -> str:
        """Path warehouse cho Iceberg catalog."""
        common = ConfigLoader.get_common_config()
        return f"s3a://{common.ceph.bucket_lakehouse}/warehouse"
