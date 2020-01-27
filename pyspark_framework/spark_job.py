import os
from abc import ABC, abstractmethod
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkContextManager:

    def __init__(self, app_name, s3_support=False):
        self.app_name = app_name
        self.conf = SparkConf()
        self.s3_support = s3_support
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        self.conf.setAppName(self.app_name)

        if self.s3_support:
            self.conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')

        spark_session = SparkSession.builder.config(conf=self.conf).getOrCreate()

        if self.s3_support:
            spark_session._jsc.hadoopConfiguration().set('fs.s3a.access.key', os.environ.get('AWS_ACCESS_KEY_ID'))
            spark_session._jsc.hadoopConfiguration().set('fs.s3a.secret.key', os.environ.get('AWS_SECRET_ACCESS_KEY'))
            spark_session._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

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
