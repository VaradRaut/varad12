from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("testing").\
    config('spark.driver.extraClassPath',"D:\\SparkSnowflake-20220329T141907Z-001\\SparkSnowflake\\spark_project\\drivers\\*").\
    config('spark.executor.extraClassPath',"D:\\SparkSnowflake-20220329T141907Z-001\\SparkSnowflake\\spark_project\\drivers\\*").getOrCreate()
#pandas_df = pd.read_csv('D:\\bigdata\\spark_project\\use.csv', names = ['tblnm','full_loadtype'])
s_df = spark.read.format("csv").option("header","true").load("D:\\SparkSnowflake-20220329T141907Z-001\\SparkSnowflake\\spark_project\\input.csv")
s_df.show()
print('varad')
for dfnew in s_df.rdd.collect():
    file_name = dfnew.tblnm+".sql.txt"
    tgtTblora = open("D:\\SparkSnowflake-20220329T141907Z-001\\SparkSnowflake\\spark_project\\sql\\" + file_name, "r").read()
    # ora_url = "jdbc:oracle:thin:@//oradb.crrnweauhzur.ap-south-1.rds.amazonaws.com:1521/ORCL"
    ora_url = "jdbc:oracle:thin:@//sanjayoracle.cq8iqbvz0eol.us-east-2.rds.amazonaws.com:1521/ORCL"
    # ora_tmp = spark.read.format("jdbc").option("url", ora_url).option("user", "orauser").option\
    #     ("password","orapassword").option("dbtable", tgtTblora).option("driver", "oracle.jdbc.OracleDriver").load()
    ora_tmp = spark.read.format("jdbc").option("url", ora_url).option("user", "ouser").option \
        ("password", "opassword").option("dbtable", tgtTblora).option("driver", "oracle.jdbc.OracleDriver").load()
    ora_tmp.show()
    ora_tmp.write.format("csv").save("D:\\SparkSnowflake-20220329T141907Z-001\\SparkSnowflake\\spark_project\\output_new2\\"+dfnew.tblnm+'\\oracle_extract_'+dfnew.tblnm+'.csv')
    # ora_tmp.toPandas().to_csv('D:\\bigdata\\spark_project\\output2\\'+dfnew.tblnm+'\\oracle_extract_'+dfnew.tblnm+'.csv')