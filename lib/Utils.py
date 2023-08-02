from pyspark.sql import SparkSession


def get_spark_session(env):

        return SparkSession.builder \
            .enableHiveSupport() \
            .getOrCreate()


