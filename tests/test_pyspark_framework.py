import os
import sys
import glob
import mock
import pytest
import yaml
from pyspark_framework.spark_run import main
from pyspark_framework.spark_job import SparkContextManager
from test_jobs.example_spark_job import ReportJob
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
        spark_session._jsc.hadoopConfiguration().set('fs.s3a.access.key', os.environ.get('AWS_ACCESS_KEY_ID'))
        spark_session._jsc.hadoopConfiguration().set('fs.s3a.secret.key', os.environ.get('AWS_SECRET_ACCESS_KEY'))
        spark_session._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

    return spark_session


def test_runner():
    """
    Run an end-to-end test of a spark job using the Spark runner
    :return:
    """
    output_dir = 'test_data/output/test_runner'
    test_args = ['test_prog', 'test_jobs.example_spark_job.ReportJob',
                 '--input-dir', 'test_data/test_data.json',
                 '--output-dir', output_dir]

    with mock.patch.object(sys, 'argv', test_args):
        main()

    output_files = glob.glob(os.path.join(output_dir, '*.parquet'))

    assert len(output_files) > 0


def test_a_job(spark_session):
    """
    Run a simple job directly to demonstrate the abstract base class in action
    :param spark_session: a local Spark session (uses fixture)
    :return:
    """
    output_dir = 'test_data/output/test_a_job'
    test_args = ['--input-dir', 'test_data/test_data.json',
                 '--output-dir', output_dir]
    job = ReportJob(spark=spark_session, config=test_args)
    job.execute()
    output_files = glob.glob(os.path.join(output_dir, '*.parquet'))

    assert len(output_files) > 0


def test_create_spark_session_no_s3():
    """
    Create a Spark session without access to S3
    :return:
    """
    with pytest.raises(Exception) as exc:
        with SparkContextManager('test') as no_s3:
            no_s3.read.parquet('s3a://wr-datalake-staging/spark_test/some_data')
    assert 'Class org.apache.hadoop.fs.s3a.S3AFileSystem not found' in str(exc.value)


def test_livy_integration():
    pass
