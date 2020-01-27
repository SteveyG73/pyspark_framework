import argparse
from pyspark_framework.spark_job import SparkJob


class ReportJob(SparkJob):

    def parse_args(self, config_list) -> dict:
        argp = argparse.ArgumentParser()
        argp.add_argument('--input-dir')
        argp.add_argument('--output-dir')
        config = vars(argp.parse_args(config_list))
        return config

    def execute(self):
        output_dir = self.config['output_dir']
        input_dir = self.config['input_dir']
        df = self.spark.read.json(input_dir)
        df.write.mode('overwrite').parquet(output_dir)
