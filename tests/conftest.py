import pytest

from pyspark import SparkConf
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark_session():
    """
    Create a spark session to be used in testing jobs locally
    :return spark_session: SparkSession
    """

    app_name = 'Local Spark test harness'
    conf = SparkConf()
    s3_support = False

    conf.setAppName(app_name)

    if s3_support:
        conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.3')

    spark_session = SparkSession.builder.config(conf=conf).getOrCreate()

    if s3_support:
        spark_session._jsc.hadoopConfiguration().set('fs.s3a.access.key',
                                                     os.environ.get(
                                                         'AWS_ACCESS_KEY_ID'))
        spark_session._jsc.hadoopConfiguration().set('fs.s3a.secret.key',
                                                     os.environ.get(
                                                         'AWS_SECRET_ACCESS_KEY'))
        spark_session._jsc.hadoopConfiguration().set('fs.s3a.impl',
                                                     'org.apache.hadoop.fs.s3a.S3AFileSystem')

    return spark_session
