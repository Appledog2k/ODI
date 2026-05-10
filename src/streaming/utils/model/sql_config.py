from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class JobMeta:
    name: str = "unknown_job"

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "JobMeta":
        if not d:
            return cls()
        return cls(name=d.get("name", "unknown_job"))


@dataclass
class SparkConfig:
    shuffle_partitions: int = 6
    default_parallelism: int = 6
    trigger_interval: str = "1 seconds"
    min_batches_to_retain: int = 5
    no_data_progress_event_interval: int = 100000
    no_data_micro_batches_enabled: bool = True

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SparkConfig":
        if not d:
            return cls()
        return cls(
            shuffle_partitions=int(d.get("shuffle_partitions", 6)),
            default_parallelism=int(d.get("default_parallelism", 6)),
            trigger_interval=str(d.get("trigger_interval", "1 seconds")),
            min_batches_to_retain=int(d.get("min_batches_to_retain", 5)),
            no_data_progress_event_interval=int(d.get("no_data_progress_event_interval", 100000)),
            no_data_micro_batches_enabled=bool(d.get("no_data_micro_batches_enabled", True)),
        )


@dataclass
class TopicKafkaConfig:
    topics_in: str = ""  # tên topic + dùng làm tên temp view
    auto_offset_reset: str = "latest"  # earliest | latest
    max_offsets_per_trigger: int = 1000
    # Optional — schema cho parse JSON (nếu có thì khai báo trong YAML)
    json_structure: List[Dict[str, str]] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TopicKafkaConfig":
        if not d:
            return cls()
        return cls(
            topics_in=d.get("topics_in", ""),
            auto_offset_reset=d.get("auto_offset_reset", "latest"),
            max_offsets_per_trigger=int(d.get("max_offsets_per_trigger", 1000)),
            json_structure=list(d.get("json_structure", [])),
        )

    @property
    def temp_view(self) -> str:
        """Theo comment YAML: topics_in dùng làm tên temp table spark."""
        return self.topics_in


@dataclass
class OutputConfig:
    sql_conditions: Optional[str] = None
    # Optional — bổ sung cho ghi Iceberg
    target_table: str = ""  # vd: lakehouse.bronze.fetch_xxx
    write_mode: str = "append"
    checkpoint_location: Optional[str] = None
    columns: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "OutputConfig":
        if not d:
            return cls()
        return cls(
            sql_conditions=d.get("sql_conditions"),
            target_table=d.get("target_table", ""),
            write_mode=d.get("write_mode", "append"),
            checkpoint_location=d.get("checkpoint_location"),
            columns=list(d.get("columns", [])),
        )


@dataclass
class SqlConfig:
    job: JobMeta = field(default_factory=JobMeta)
    spark: SparkConfig = field(default_factory=SparkConfig)
    kafka: TopicKafkaConfig = field(default_factory=TopicKafkaConfig)
    output: OutputConfig = field(default_factory=OutputConfig)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SqlConfig":
        if not d:
            return cls()
        return cls(
            job=JobMeta.from_dict(d.get("job", {})),
            spark=SparkConfig.from_dict(d.get("spark", {})),
            kafka=TopicKafkaConfig.from_dict(d.get("kafka", {})),
            output=OutputConfig.from_dict(d.get("output", {})),
        )
