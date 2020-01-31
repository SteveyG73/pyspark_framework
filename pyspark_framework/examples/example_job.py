import argparse
from pyspark_framework.spark_job import SparkJob

from pyspark.sql.functions import desc, input_file_name
from pyspark.sql.types import IntegerType


class FlightSummary(SparkJob):

    def parse_args(self, arg_list):
        argp = argparse.ArgumentParser()
        argp.add_argument('--input', required=True)
        argp.add_argument('--output', required=False)
        return vars(argp.parse_args(arg_list))

    def add_year(self, df):
        df2 = (
            df.withColumn('flight_year',
                          input_file_name().substr(1, 4).cast(IntegerType()))
        )
        return df2

    def get_top_10_destinations(self, df):
        df2 = (df.groupBy('DEST_COUNTRY_NAME')
               .sum('count')
               .withColumnRenamed('sum(count)', 'total_flights')
               .orderBy(desc('total_flights'))
               .limit(10)
               )
        return df2

    def execute(self):
        raw_data = self.spark.read.csv(self.config.get('input'), sep=",",
                                       header=True, inferSchema=True)
        enriched_data = self.add_year(raw_data)
        enriched_data.printSchema()
        top_10_destinations = self.get_top_10_destinations(enriched_data)
        top_10_destinations.show(10, truncate=False)
