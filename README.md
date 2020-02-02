# pyspark_framework

## Overview
This is a light-weight framework for running an Apache Spark job either locally or against a cluster.

It's designed primarily for use with AWS EMR.

The design intention is to abstract away interacting with the Spark context so the report writer just has to write their application code and nothing else.

## Usage

First install the framework:

```bash
pip install pyspark_framework
```

Then create a new project to house your Spark jobs along the lines of:

```text
project_name/
|-- jobs/
    |-- spark_job.py            
```

To implement the framework, just create a class that extends `SparkJob`:

```python
from pyspark_framework.spark_job import SparkJob

class MySparkJob(SparkJob):

  def execute(self):
      '''
      This implements an abstract method from the base class SparkJob
      '''
      data = self.spark.read.json("my/stuff")

```

You can access the Spark session via the instance property `self.spark`.

Clean-up of the Spark session is automatically handled so you don't need to do anything else!

Input parameters to the job can be implemented by overriding the `parse_args` method. Arguments from the command line are passed as a list that you can write your own `argparse` against.

The list passed in comes from the remainder of `sys.argv` after the framework parameters have been removed (uses `parse_known_args`).

Example:
```python
    def parse_args(self, arg_list):
        argp = argparse.ArgumentParser()
        argp.add_argument('--input', required=True)
        argp.add_argument('--output', required=False)
        return vars(argp.parse_args(arg_list))
```

A complete example can be found [here](https://github.com/SteveyG73/pyspark_framework/tree/master/pyspark_framework/examples).
