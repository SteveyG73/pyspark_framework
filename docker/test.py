from pyspark_framework import spark_job

if __name__ == '__main__':
    with spark_job.SparkContextManager('test') as spark:
        print(spark.version)
