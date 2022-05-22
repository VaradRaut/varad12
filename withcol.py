from pyspark.sql import *
from pyspark.sql.functions import *

rdd1 = spark.sparkContext.textFile("D:\\sparks\\datasets-20220228T155727Z-001\\datasets\\companies.json")

rdd2 = spark.sparkContext