from pyspark.sql import *
from Spark_1.sample._utilis_code import *
import unittest

class PysparkUnittest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark=SparkSession.builder.master('local').appName('test_code').getOrCreate()
        cls.spark=spark
    def test_user(self):
        sample = user(self.spark)
        result = sample.count()
        self.assertEqual(result,10)
    def test_transcation(self):
        sample = transaction(self.spark)
        result = sample.count()
        self.assertEqual(result,10)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()