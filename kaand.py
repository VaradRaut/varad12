from pyspark.sql import *
from pyspark.sql.functions import lit,when,regexp_replace,col,current_date

spark = SparkSession.builder.master('local[*]').appName('practice').getOrCreate()

#rdd1 = spark.sparkContext.textFile("D:\\sparks\\datasets-20220228T155727Z-001\\datasets\\companies.json")
#print(rdd1.collect())


df = spark.read.format('csv').option("header","true").option("inferSchema","true").load("D:\\sparks\\datasets-20220228T155727Z-001\\datasets\\employees.csv")
df1 = df.select(*(regexp_replace(col(c),"\'","").alias(c)for c in df.columns))\
    .withColumn("today",current_date())\
    .withColumn("Age",lit(18))\
    .withColumn("Age",when(col(" Country").like('%UK%'),25).otherwise(col("Age")))
df1.show()
df1.printSchema()




#rdd1 = spark.sparkContext.textFile("D:\\sparks\\datasets-20220228T155727Z-001\\datasets\\Employee\\employees01.csv")
#print(rdd1.collect())

#df = rdd1.toDF("Lem","Boissier","lboissier@sf_tuts.com","3002 Ruskin Trail","Shikārpur","8/25/2017")
#df.printSchema()
#df.show()


