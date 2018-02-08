from pyspark import SparkContext

from pyspark.sql import HiveContext

sc = SparkContext()
hc = HiveContext(sc)

class SparkHiveContext:


    def getSparkContext(self):

        return sc

    def getHiveContext(self):

        return hc

