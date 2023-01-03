# Import Lib
from pyspark import SparkConf, SparkContext

# Set up Context
conf = SparkConf().setMaster("local").setAppName("FrindsByAge")
sc = SparkContext(conf = conf)

# Data Cleaning def function
def parseLine (line):
    fields =line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# Load Data
lines = sc.textFile("file:///SparkCourse/fakefriends.csv")

rdd = lines.map(parseLine)

# Count Up Sum of Friends and Number of Entries per Age 
totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

# Collect and Display the result
for result in averagesByAge.collect():
    print(result)