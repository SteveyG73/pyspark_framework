import os

AWS_JAR_FILE = 'org.apache.hadoop:hadoop-aws:2.7.3'
S3_FILE_SYSTEM_CLASS = 'org.apache.hadoop.fs.s3a.S3AFileSystem'


def configure_aws_jars(spark_conf):
    spark_conf.set('spark.jars.packages', AWS_JAR_FILE)
    return spark_conf


def configure_aws_credentials(spark_session):
    spark_session._jsc.hadoopConfiguration().set('fs.s3a.access.key',
                                                 os.environ.get(
                                                     'AWS_ACCESS_KEY_ID'))
    spark_session._jsc.hadoopConfiguration().set('fs.s3a.secret.key',
                                                 os.environ.get(
                                                     'AWS_SECRET_ACCESS_KEY'))
    spark_session._jsc.hadoopConfiguration().set('fs.s3a.impl',
                                                 S3_FILE_SYSTEM_CLASS)
    return spark_session
