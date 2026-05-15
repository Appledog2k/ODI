import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

from streaming.utils.model.common_config import CommonConfig
from streaming.utils.model.sql_config import SqlConfig
from streaming.utils.string_constants import StringConstants

logger = logging.getLogger(__name__)

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


def _resolve_env(value: Any) -> Any:
    if isinstance(value, str):
        def replacer(m: re.Match) -> str:
            var_name = m.group(1)
            default = m.group(2)
            env_val = os.getenv(var_name, default)
            if env_val is None:
                raise EnvironmentError(
                    f"Missing required environment variable: {var_name}"
                )
            return env_val

        return _ENV_PATTERN.sub(replacer, value)
    if isinstance(value, dict):
        return {k: _resolve_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env(v) for v in value]
    return value


def _read_yaml(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(p, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return _resolve_env(raw)


def _read_properties_file(path: Optional[str]) -> Dict[str, str]:
    if not path:
        return {}

    p = Path(path)
    if not p.exists():
        logger.warning(f"Properties file not found: {path}")
        return {}

    properties = {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                # Bỏ qua comment và dòng trống
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    if key and value:  # Chỉ thêm nếu key và value không rỗng
                        properties[key] = value
        logger.info(f"Loaded {len(properties)} properties from {path}")
    except Exception as e:
        logger.error(f"Error reading properties file {path}: {e}")

    return properties


def _load_env_variables(properties_dict: Dict[str, str]) -> None:
    """
    Load các biến vào os.environ theo thứ tự ưu tiên:
    1. application.properties file (Priority cao nhất - override OS env)
    2. OS environment variables (Priority thấp nhất)

    Cách hoạt động:
    - Nếu biến có trong properties file → load từ properties file (override OS env)
    - Nếu biến không có trong properties file + có trong OS env → giữ nguyên OS env
    - Nếu biến không có ở cả hai nơi → chỉ log warning
    """
    required_keys = [
        StringConstants.KAFKA_BOOTSTRAP_SERVERS,
        StringConstants.KAFKA_USER,
        StringConstants.KAFKA_PASSWORD,
        "HIVE_URI",
        StringConstants.CEPH_ENDPOINT,
        StringConstants.CEPH_ACCESS_KEY,
        StringConstants.CEPH_SECRET_KEY,
        "CEPH_BUCKET_CHECKPOINTS",
        "CEPH_BUCKET_LAKEHOUSE",
    ]

    for key in required_keys:
        if key in properties_dict:
            # Nếu có trong properties file → lấy từ đó (priority cao - override OS env)
            os.environ[key] = properties_dict[key]
            logger.info(f"✓ {key} = {properties_dict[key][:20]}... (từ application.properties)")
        elif os.getenv(key):
            # Nếu không có trong properties file + có trong OS env → giữ nguyên
            env_value = os.getenv(key)
            logger.info(f"✓ {key} = {env_value[:20]}... (từ OS environment)")
        else:
            # Không tìm thấy ở cả hai nơi
            logger.warning(f"⚠ {key} không tìm thấy trong application.properties hoặc OS env")



class ConfigLoader:
    _common_config: Optional[CommonConfig] = None
    _sql_config: Optional[SqlConfig] = None
    _initialized: bool = False

    @classmethod
    def initialize(
        cls,
        config_file_path: str,
        sql_file_path: str,
        properties_file_path: Optional[str] = None
    ) -> None:
        if cls._initialized:
            logger.info("ConfigLoader đã được initialize, skip.")
            return

        # Load properties file và set environment variables nếu chưa tồn tại
        if properties_file_path:
            logger.info(f"Loading properties from: {properties_file_path}")
            properties_dict = _read_properties_file(properties_file_path)
            _load_env_variables(properties_dict)
        else:
            logger.info("No properties file specified, using environment variables only")

        logger.info(f"Loading common config: {config_file_path}")
        common_raw = _read_yaml(config_file_path)
        cls._common_config = CommonConfig.from_dict(common_raw)

        logger.info(f"Loading SQL config: {sql_file_path}")
        sql_raw = _read_yaml(sql_file_path)
        cls._sql_config = SqlConfig.from_dict(sql_raw)

        cls._initialized = True
        logger.info(
            f"ConfigLoader initialized — job={cls._sql_config.job.name}, "
            f"topic={cls._sql_config.kafka.topics_in}"
        )


    @classmethod
    def get_common_config(cls) -> CommonConfig | None:
        cls._ensure()
        return cls._common_config

    @classmethod
    def get_sql_config(cls) -> SqlConfig | None:
        cls._ensure()
        return cls._sql_config

    @classmethod
    def reset(cls) -> None:
        cls._common_config = None
        cls._sql_config = None
        cls._initialized = False

    @classmethod
    def _ensure(cls) -> None:
        if not cls._initialized:
            raise RuntimeError("ConfigLoader chưa được initialize().")