import json
import logging.config
import os
import sys
from datetime import datetime
from pathlib import Path

import logging


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "thread": record.threadName,
            "job": os.getenv("JOB_NAME", "unknown-job"),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        for k, v in record.__dict__.items():
            if k.startswith("ctx_"):
                payload[k[4:]] = v
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(
        job_name: str,
        log_level: str = None,
        log_dir: str = None,
        enable_json: bool = True,
) -> logging.Logger:
    """
    Khởi tạo logging cho ứng dụng.
    Args ưu tiên > env vars > default.
    """
    log_level = log_level or os.getenv("LOG_LEVEL", "INFO")
    log_dir = log_dir or os.getenv("LOG_DIR", "/var/log/spark")

    os.environ["JOB_NAME"] = job_name
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    text_fmt = (
        "%(asctime)s [%(levelname)s] %(name)s "
        f"[job={job_name}] - %(message)s"
    )

    handlers = {
        "console": {
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
            "formatter": "text",
            "level": log_level,
        },
        "file_text": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": f"{log_dir}/{job_name}.python.log",
            "maxBytes": 100 * 1024 * 1024,  # 100MB
            "backupCount": 10,
            "formatter": "text",
            "level": log_level,
            "encoding": "utf-8",
        },
    }

    if enable_json:
        handlers["file_json"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": f"{log_dir}/{job_name}.python.json.log",
            "maxBytes": 100 * 1024 * 1024,
            "backupCount": 10,
            "formatter": "json",
            "level": log_level,
            "encoding": "utf-8",
        }

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "text": {"format": text_fmt},
            "json": {"()": f"{__name__}.JsonFormatter"},
        },
        "handlers": handlers,
        "loggers": {
            "py4j": {"level": "WARN"},
            "kafka": {"level": "WARN"},
            "botocore": {"level": "WARN"},
            "urllib3": {"level": "WARN"},
        },
        "root": {
            "level": log_level,
            "handlers": list(handlers.keys()),
        },
    }

    logging.config.dictConfig(config)
    logger = logging.getLogger("streaming")
    logger.info(f"Logging initialized for job={job_name}, level={log_level}, dir={log_dir}")
    return logger
