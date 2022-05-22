
from pyspark.sql import *
from pyspark.sql import functions as F
import re
from pyspark.streaming import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 10)

lines = ssc.socketTextStream("ec2-15-207-248-239.ap-south-1.compute.amazonaws.com", 1235)
lines.pprint()
#socket .. terminal ..get streaming data from terminal
#something server generate data from 9999 host  port number


ssc.start()             # Start the computation
ssc.awaitTermination() # Wait for the computation to terminate