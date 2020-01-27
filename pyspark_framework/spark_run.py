import sys
import importlib
import argparse

from .spark_job import SparkContextManager


def main():
    job_args, app_args = get_args(sys.argv[1:])
    job_class_ref = get_job_class_ref(job_args.job_class_name)

    with SparkContextManager(job_args.app_name, job_args.s3) as spark:
        job = job_class_ref(spark, app_args)
        job.execute()


def get_args(args):
    argp = argparse.ArgumentParser(prog='Spark Job Runner')
    argp.add_argument('job_class_name', help='Fully qualified job class e.g. package.module.ClassName')
    argp.add_argument('--app-name', '-a', default='pyspark-job', help='Application name to send to Spark Session')
    argp.add_argument('--s3', action='store_true', help='Specify local S3 support')
    return argp.parse_known_args(args)


def get_job_class_ref(job_to_run):
    job_parts = job_to_run.split('.')
    job_class = job_parts[-1]
    job_module = '.'.join(job_parts[0:len(job_parts) - 1])
    try:
        module = importlib.import_module(job_module)
        class_to_run = getattr(module, job_class)
    except ModuleNotFoundError:
        raise ModuleNotFoundError(f"Module {job_module} cannot be found")
    return class_to_run


if __name__ == '__main__':
    main()
