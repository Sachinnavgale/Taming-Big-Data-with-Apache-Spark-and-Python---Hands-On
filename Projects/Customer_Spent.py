# Import Required Lib
from pyspark import SparkConf, SparkContext

# Set up Context
conf = SparkConf().setMaster("local").setAppName("CustomerSpent")
sc = SparkContext(conf = conf)

# Data cleaning def function
def parsedline(line):
    fields = line.split(',')
    return(int(fields[0], float(fields[2])))
           
# Load Data
input = sc.textFile("file:///SparkCourse/customer-order.csv")
mappedinput = input.map(parsedline)
totalBuCustomer = mappedinput.reduceByKey(lambda x, y: x + y)

# Changed for python 3 compatility:
# flipped = totalByCostomer.map(lambda (x,y):(y,x))
flipped = totalBuCustomer.map(lambda x: (x[1], x [0]))

# Sort by Customer spending
sortedcustomerspent = flipped.sortByKey()

# Collect and display the result
for spent in sortedcustomerspent.collect():
    print(spent)