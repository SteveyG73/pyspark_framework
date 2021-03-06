import argparse
from pyspark_framework.spark_job import SparkJob

from pyspark.sql.functions import desc, input_file_name, split, slice, col
from pyspark.sql.types import IntegerType


class FlightSummary(SparkJob):

    def parse_args(self, arg_list):
        argp = argparse.ArgumentParser()
        argp.add_argument('--input', required=True)
        argp.add_argument('--output', required=False)
        return vars(argp.parse_args(arg_list))

    def add_year(self, df):
        df2 = (
            df.withColumn('file_name', slice(split(input_file_name(), '/'),
                                             -1 ,1)[0])
                .withColumn('flight_year',
                            col('file_name').substr(1, 4).cast(IntegerType()))
        )
        return df2

    def get_top_destinations(self, df):
        df2 = (df.groupBy('DEST_COUNTRY_NAME', 'flight_year')
               .sum('count')
               .withColumnRenamed('sum(count)', 'total_flights')
               .orderBy(desc('total_flights'))
               .limit(50)
               )
        return df2

    def pivot_on_year(self, df):
        df2 = (
            df.groupBy('DEST_COUNTRY_NAME').pivot('flight_year').sum(
                'total_flights')
            .na.fill(0)
        )
        return df2

    def execute(self):
        raw_data = self.spark.read.csv(self.config.get('input'), sep=",",
                                       header=True, inferSchema=True)
        enriched_data = self.add_year(raw_data)
        enriched_data.printSchema()
        top_10_destinations = self.get_top_destinations(enriched_data)
        pivoted_data = self.pivot_on_year(top_10_destinations)
        pivoted_data.show(10, truncate=False)
