import os

from common.base import DataHandler


class DataConnector(DataHandler):
    def __init__(self, spark, config):
        super().__init__(spark, config)
