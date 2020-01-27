##How to run the example:

To run directly:

```bash
python driver.py \
       pyspark_framework.examples.example_job.EmarsysReportOpenedEmails \
       --input pyspark_framework/examples/example_data/ \
       --output=/tmp/spark/tests \
       --base-date=2018-01-01
```

...or if you want to run it via the installed module:

```bash
python -m pyspark_framework.spark_run \
          pyspark_framework.examples.example_job.EmarsysReportOpenedEmails \
          --input pyspark_framework/examples/example_data/ \
          --output=/tmp/spark/tests \
          --base-date=2018-01-01 
```