from setuptools import setup

with open('requirements.txt', 'r') as r:
    requirements = r.read().split('\n')

setup(name='pyspark_framework',
      version='0.1',
      description='PySpark job runner',
      url='',
      author='Steve Garner',
      author_email='sgarner@worldremit.com',
      license='MIT',
      packages=['pyspark_framework'],
      install_requires=requirements,
      zip_safe=False,
      entry_points={
          "console_scripts": [
              "spark-run = pyspark_framework.spark_run:main"
          ]
      })
