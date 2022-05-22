from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()



#Create RDD from parallelize
data = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd=spark.sparkContext.parallelize(data)

rdd1= rdd.map(lambda x: (x,1))

rdd2 = rdd.flatMap(lambda x: (x,1))
rdd3 = rdd.reduceByKey(lambda x,y: x+y)

print(rdd3.collect())


