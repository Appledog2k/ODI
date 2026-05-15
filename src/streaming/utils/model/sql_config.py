from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List


@dataclass
class Job:
    name: str = "unknown_job"

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Job":
        if not d:
            return cls()
        return cls(
            name=d.get("name", "unknown_job")
        )

    @property
    def temp_view(self) -> str:
        return self.name


@dataclass
class SparkConfig:
    shuffle_partitions: int = 6
    default_parallelism: int = 6
    trigger_interval: str = "1 seconds"
    min_batches_to_retain: int = 5
    no_data_progress_event_interval: int = 100000

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SparkConfig":
        if not d:
            return cls()
        return cls(
            shuffle_partitions=int(d.get("shuffle_partitions", 6)),
            default_parallelism=int(d.get("default_parallelism", 6)),
            trigger_interval=str(d.get("trigger_interval", "1 seconds")),
            min_batches_to_retain=int(d.get("min_batches_to_retain", 5)),
            no_data_progress_event_interval=int(
                d.get("no_data_progress_event_interval", 100000)
            ),
        )


@dataclass
class FieldSchema:
    name: str = ""
    type: str = "string"

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "FieldSchema":
        if not d:
            return cls()
        return cls(
            name=d.get("name", ""),
            type=d.get("type", "string"),
        )


@dataclass
class SchemaConfig:
    fields: List[FieldSchema] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SchemaConfig":
        if not d:
            return cls()
        raw_fields = d.get("fields", []) or []
        return cls(
            fields=[FieldSchema.from_dict(f) for f in raw_fields],
        )

    def to_field_map(self) -> Dict[str, str]:
        """Trả về dict {name: type} cho tiện build StructType."""
        return {f.name: f.type for f in self.fields}


@dataclass
class TopicKafkaConfig:
    topics_in: str = ""
    auto_offset_reset: str = "latest"  # earliest | latest
    max_offsets_per_trigger: int = 1000
    data_format: str = "json"  # json | avro
    schema_cdc: SchemaConfig = field(default_factory=SchemaConfig)
    schema_data: SchemaConfig = field(default_factory=SchemaConfig)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TopicKafkaConfig":
        if not d:
            return cls()
        return cls(
            topics_in=d.get("topics_in", ""),
            auto_offset_reset=d.get("auto_offset_reset", "latest"),
            max_offsets_per_trigger=int(d.get("max_offsets_per_trigger", 1000)),
            data_format=d.get("data_format", "json"),
            schema_cdc=SchemaConfig.from_dict(d.get("schema_cdc", {})),
            schema_data=SchemaConfig.from_dict(d.get("schema_data", {})),
        )


@dataclass
class OutputConfig:
    sql_conditions: Optional[str] = None
    target_table: str = ""  # vd: lakehouse.bronze.fetch_xxx
    write_mode: str = "append"

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "OutputConfig":
        if not d:
            return cls()
        return cls(
            sql_conditions=d.get("sql_conditions"),
            target_table=d.get("target_table", ""),
            write_mode=d.get("write_mode", "append"),
        )


@dataclass
class SqlConfig:
    job: Job = field(default_factory=Job)
    spark: SparkConfig = field(default_factory=SparkConfig)
    kafka: TopicKafkaConfig = field(default_factory=TopicKafkaConfig)
    output: OutputConfig = field(default_factory=OutputConfig)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SqlConfig":
        if not d:
            return cls()
        return cls(
            job=Job.from_dict(d.get("job", {})),
            spark=SparkConfig.from_dict(d.get("spark", {})),
            kafka=TopicKafkaConfig.from_dict(d.get("kafka", {})),
            output=OutputConfig.from_dict(d.get("output", {})),
        )
