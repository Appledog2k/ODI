from pyspark.sql import SparkSession


class DataHandler:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
