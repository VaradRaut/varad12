from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()


rdd1 = spark.sparkContext.textFile("D:\\sparks\\datasets-20220228T155727Z-001\\datasets\\companies.json")

rdd2 = rdd1.flatMap(lambda x: x.split (" "))

rdd3 = rdd1.map(lambda x: (x,1))
print(rdd3.collect())