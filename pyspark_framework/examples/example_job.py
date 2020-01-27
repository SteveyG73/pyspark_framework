import argparse
from pyspark_framework.spark_job import SparkJob
from pyspark.sql.functions import col, count
from pyspark.sql import Window


class EmarsysReportOpenedEmails(SparkJob):

    def get_count_by_campaign(self, df):
        df2 = (df
               .groupby(df['campaign_id'])
               .agg(count('campaign_id')
                    .alias('campaign_count'))
               .orderBy(col('campaign_count').desc_nulls_last())
               )
        return df2

    def get_filtered_by_date(self, df, base_date):
        df2 = (df
               .filter(df.event_date >= base_date)
               )
        return df2

    def deduplicate(self, df):
        df2 = df.distinct()
        return df2

    def parse_args(self, arg_list):
        argp = argparse.ArgumentParser()
        argp.add_argument('--input', required=True)
        argp.add_argument('--output', required=True)
        argp.add_argument('--base-date', default='2018-01-01')
        return vars(argp.parse_args(arg_list))

    def execute(self):
        raw_data = self.spark.read.csv(self.config.get('input'), sep=";", header=True)
        filtered_by_date = self.get_filtered_by_date(raw_data, self.config.get('base_date'))
        deduped = self.deduplicate(filtered_by_date)
        counts_by_customer = self.get_count_by_campaign(deduped)
        counts_by_customer.write.parquet(self.config.get('output'), mode='overwrite')
        counts_by_customer.show(10, truncate=False)

