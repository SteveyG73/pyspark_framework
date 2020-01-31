from abc import ABC, abstractmethod
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark_framework import aws_support


class SparkContextManager:

    def __init__(self, app_name, s3_support=False):
        self.app_name = app_name
        self.conf = SparkConf()
        self.s3_support = s3_support
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        self.conf.setAppName(self.app_name)

        if self.s3_support:
            self.conf = aws_support.configure_aws_jars(self.conf)

        spark_session = (SparkSession
                         .builder
                         .config(conf=self.conf)
                         .getOrCreate())

        if self.s3_support:
            spark_session = (aws_support
                             .configure_aws_credentials(spark_session))

        return spark_session

    def __enter__(self):
        return self.spark

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.spark.stop()


class SparkJob(ABC):

    def __init__(self, spark, config):
        super().__init__()
        self.spark = spark
        self.config = self.parse_args(config)

    def parse_args(self, config_list) -> dict:
        return {}

    @abstractmethod
    def execute(self):
        pass
