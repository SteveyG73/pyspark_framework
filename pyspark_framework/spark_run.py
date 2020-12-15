import sys
import os
import importlib
import argparse

from pyspark_framework.spark_job import SparkContextManager

"""
This module provides an easy method of running a Spark job by just supplying
the module and class reference. The arguments are derived from sys.argv.
"""


def main():
    """
    Main entry point for running a Spark job
    Imports the required class, creates a Spark session and then runs then
    job
    :return:
    """
    job_args, app_args = get_args(sys.argv[1:])
    job_class_ref = get_job_class_ref(job_args.job_class_name)

    with SparkContextManager(job_args.app_name, job_args.s3) as spark:
        job = job_class_ref(spark, app_args)
        sys.exit(job.execute())


def get_args(args):
    """
    Takes a list of arguments and splits them out into the known args for
    Pyspark-Framework and returns a tuple with the parsed arguments as a
    Namespace and the remaining args as a list
    :param args: Raw List() of arguments in the format of sys.argv
    :return: Tuple of (Namespace, List) where the list is the remaining unknown
             arguments
    """

    argp = argparse.ArgumentParser(prog='Spark Job Runner')
    argp.add_argument('--class',
                      help='Fully qualified job class '
                           'e.g. package.module.ClassName',
                      dest="job_class_name",
                      required=True)
    argp.add_argument('--app-name', '-a', default='pyspark-job',
                      help='Application name to send to Spark Session')
    argp.add_argument('--s3', action='store_true',
                      help='Specify local S3 support')
    return argp.parse_known_args(args)


def get_job_class_ref(job_to_run):
    """
    Tracks down the class requested to be run and returns a reference to it
    :param job_to_run: String of a fully qualified reference e.g.
           "jobs.job_module.ReportClass"
    :return: the object reference for the class
    """
    # Make sure the current working directory is in the sys.path to allow for
    # easy importing of local packages and modules
    wrk_dir = os.getcwd()
    if wrk_dir not in sys.path:
        sys.path.append(wrk_dir)

    # Slice the job reference into its component parts
    job_parts = job_to_run.split('.')
    job_class = job_parts[-1]
    job_module = '.'.join(job_parts[:-1])

    # Attempt to import the module and find the class reference
    try:
        module = importlib.import_module(job_module)
        class_to_run = getattr(module, job_class)
    except ModuleNotFoundError:
        raise ModuleNotFoundError(f"Module {job_module} cannot be found")
    return class_to_run


if __name__ == '__main__':
    main()
