import logging
from abc import abstractmethod

from streaming.base.spark.spark_session_wrapper import SparkSessionWrapper
from streaming.utils.exception_handler import handle_fatal_error

logger = logging.getLogger(__name__)


class BaseApplication(SparkSessionWrapper):
    def start(self) -> None:
        try:
            logger.info(f"▶️  Starting application: {self.app_name}")
            self.init(self.spark.sparkContext)
        except SystemExit:
            raise
        except KeyboardInterrupt:
            logger.warning("⚠️ Interrupted by user")
        except Exception as e:  # noqa
            handle_fatal_error(f"Application {self.app_name} failed", e)
        finally:
            self.close()

    @abstractmethod
    def init(self, sc) -> None:
        ...