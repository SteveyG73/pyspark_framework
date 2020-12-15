## How to run the example:

To run directly:

```bash
python driver.py \
       pyspark_framework.examples.example_job.FlightSummary \
       --input pyspark_framework/examples/example_data/flight-data/csv \
       --output=/tmp/spark/tests
```

...or if you want to run it via the installed module:

```bash
python -m pyspark_framework.spark_run \
          --class pyspark_framework.examples.example_job.FlightSummary \
          --input pyspark_framework/examples/example_data/flight-data/csv \
          --output=/tmp/spark/tests
```

...or if you prefer you can use the console script:

```bash
spark-run --class pyspark_framework.examples.example_job.FlightSummary \
          --input pyspark_framework/examples/example_data/flight-data/csv \
          --output=/tmp/spark/tests          
```