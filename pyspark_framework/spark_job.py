from abc import ABC, abstractmethod
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark_framework import aws_support

"""
This module supplies the Spark context manager that handles obtaining a
Spark session.

It also includes the abstract base class that report jobs should extend. To 
create an implementation, create a class that extends the base class `SparkJob`
and then implement the `execute` method.
"""


class SparkContextManager:
    """
    Allow easy creation of a SparkSession via a simple `with...` clause. Tidies
    up the SparkSession upon exit automatically.
    """

    def __init__(self, app_name: str, s3_support: bool = False, jars: list = None):
        self.app_name = app_name
        self.conf = SparkConf()
        self.s3_support = s3_support
        self.jars = jars or list()
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        """
        Discover or create the Spark session and optionally allow access to
        S3 (this is only required if you want to run against S3 outside of EMR)
        :return: A SparkSession instance
        """
        self.conf.setAppName(self.app_name)
        jar_list = ",".join(self.jars)
        self.conf.set("spark.jars", jar_list)
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
    """
    Abstract base class that allows a simple method of creating a new Spark
    job without having to worry about the Spark Session. Stores the Spark
    session in a convenient property for easy access.

    Additionally, it's possible to override the default `parse_args` method
    with a custom version to handle any additional application level arguments.
    """

    def __init__(self, spark, config):
        super().__init__()
        self.spark = spark
        self.config = self.parse_args(config)

    def parse_args(self, config_list):
        """

        :param config_list: a list of program parameters in the form usually
        supplied by sys.argv
        :return: The parsed arguments in the format of your choice. Defaults
        to a dictionary.
        """
        return {}

    @abstractmethod
    def execute(self):
        """
        The main program for the Spark job.
        :return:
        """
        pass
