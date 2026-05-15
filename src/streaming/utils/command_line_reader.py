import argparse
from typing import List, Optional


class CommandLineReader:
    def __init__(self, args: Optional[List[str]] = None):
        parser = argparse.ArgumentParser(
            description="Spark Streaming Kafka → Iceberg Lakehouse"
        )
        parser.add_argument(
            "--config-file",
            required=True,
            help="Path tới common config YAML (lakehouse + kafka cluster)",
        )
        parser.add_argument(
            "--sql-file",
            required=True,
            help="Path tới SQL/job config YAML",
        )
        parser.add_argument(
            "--properties-file",
            required=False,
            default=None,
            help="Path tới application.properties file"
        )
        self._ns = parser.parse_args(args)

    def get_file_config_path(self) -> str:
        return self._ns.config_file

    def get_file_sql_path(self) -> str:
        return self._ns.sql_file
    
    def get_properties_file_path(self) -> Optional[str]:
        return self._ns.properties_file
