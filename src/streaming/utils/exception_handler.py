import logging
import sys
from typing import Optional

logger = logging.getLogger(__name__)


class ExceptionHandler:
    def __init__(self):
        raise NotImplementedError("Utility class — do not instantiate")

    @staticmethod
    def handle_fatal_error(
        error_message: str,
        exception: Optional[BaseException] = None,
        exit_code: int = 1,
    ) -> None:
        if exception is not None:
            logger.error(f"FATAL: {error_message}", exc_info=exception)
        else:
            logger.error(f"FATAL: {error_message}")
        sys.exit(exit_code)


def handle_fatal_error(
    error_message: str,
    exception: Optional[BaseException] = None,
    exit_code: int = 1,
) -> None:
    ExceptionHandler.handle_fatal_error(error_message, exception, exit_code)
