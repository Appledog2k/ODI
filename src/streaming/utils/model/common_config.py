from dataclasses import dataclass, field
from typing import Any, Dict

@dataclass
class CommonConfig:
    lakehouse: LakehouseConfig = field(default_factory=LakehouseConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CommonConfig":
        if not d:
            return cls()
        return cls(
            lakehouse=LakehouseConfig.from_dict(d.get("lakehouse", {})),
            kafka=KafkaConfig.from_dict(d.get("kafka", {})),
        )

    @property
    def ceph(self) -> CephConfig:
        return self.lakehouse.ceph

    @property
    def iceberg(self) -> IcebergConfig:
        return self.lakehouse.iceberg

@dataclass
class LakehouseConfig:
    ceph: CephConfig = field(default_factory=CephConfig)
    iceberg: IcebergConfig = field(default_factory=IcebergConfig)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "LakehouseConfig":
        if not d:
            return cls()
        return cls(
            ceph=CephConfig.from_dict(d.get("ceph", {})),
            iceberg=IcebergConfig.from_dict(d.get("iceberg", {})),
        )


@dataclass
class KafkaConfig:
    bootstrap_servers: str = ""
    security_protocol: str = "SASL_PLAINTEXT"
    sasl_mechanism: str = "SCRAM-SHA-256"
    user: str = ""
    password: str = ""
    secure: bool = False

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "KafkaConfig":
        if not d:
            return cls()
        return cls(
            bootstrap_servers=d.get("bootstrap_servers", ""),
            security_protocol=d.get("security_protocol", "SASL_PLAINTEXT"),
            sasl_mechanism=d.get("sasl_mechanism", "SCRAM-SHA-256"),
            user=d.get("user", ""),
            password=d.get("password", ""),
            secure=bool(d.get("secure", False)),
        )

@dataclass
class CephConfig:
    ceph_endpoint: str = ""
    ceph_access_key: str = ""
    ceph_secret_key: str = ""
    region: str = "default"
    ssl_enabled: bool = False
    path_style_access: bool = True

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CephConfig":
        if not d:
            return cls()
        return cls(
            ceph_endpoint=d.get("ceph_endpoint", ""),
            ceph_access_key=d.get("ceph_access_key", ""),
            ceph_secret_key=d.get("ceph_secret_key", ""),
            region=d.get("region", "default"),
            ssl_enabled=str(d.get("ssl_enabled", "false")).lower() == "true",
            path_style_access=bool(d.get("path_style_access", True)),
        )


@dataclass
class IcebergConfig:
    write_format_default: str = "parquet"
    parquet_compression_codec: str = "zstd"
    target_file_size_bytes: str = "134217728"
    distribution_mode: str = "hash"
    metadata_delete_after_commit_enabled: str = "true"
    metadata_previous_versions_max: str = "10"
    format_version: str = "2"

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "IcebergConfig":
        if not d:
            return cls()

        return cls(
            write_format_default=str(d.get("write.format.default", "parquet")),
            parquet_compression_codec=str(d.get("write.parquet.compression-codec", "zstd")),
            target_file_size_bytes=str(d.get("write.target-file-size-bytes", "134217728")),
            distribution_mode=str(d.get("write.distribution-mode", "hash")),
            metadata_delete_after_commit_enabled=str(
                d.get("write.metadata.delete-after-commit.enabled", "true")
            ),
            metadata_previous_versions_max=str(
                d.get("write.metadata.previous-versions-max", "20")
            ),
            format_version=str(d.get("format-version", "2")),
        )
