import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from streaming.utils.model.common_config import CommonConfig
from streaming.utils.model.sql_config import SqlConfig

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


class ConfigLoader:
    _common_config: Optional[CommonConfig] = None
    _sql_config: Optional[SqlConfig] = None
    _initialized: bool = False

    @classmethod
    def initialize(
        cls,
        config_file_path: str,
        sql_file_path: str,
        prefix_phone_file_path: Optional[str] = None,  # giữ tham số tương thích
    ) -> None:
        if cls._initialized:
            logger.info("ConfigLoader đã được initialize, skip.")
            return

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
    def get_common_config(cls) -> CommonConfig:
        cls._ensure()
        return cls._common_config

    @classmethod
    def get_common_config(cls) -> CommonConfig:
        cls._ensure()
        return cls._common_config

    @classmethod
    def get_sql_config(cls) -> SqlConfig:
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