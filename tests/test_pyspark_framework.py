import os
import sys
import glob
import unittest.mock as mock
import pytest
from pyspark_framework.spark_run import main
from pyspark_framework.spark_job import SparkContextManager
from test_jobs.example_spark_job import ReportJob


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
            no_s3.read.parquet('s3a://apache-zeppelin/tutorial/bank/bank.csv')
    assert 'Class org.apache.hadoop.fs.s3a.S3AFileSystem not found' in str(
        exc.value)


def test_livy_integration():
    pass
