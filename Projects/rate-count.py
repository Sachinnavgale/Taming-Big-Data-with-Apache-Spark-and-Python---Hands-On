# Import necessary Lib
from pyspark import SparkConf,SparkContext
import collections

# Set up Context
conf= SparkConf().setMaster('local').setAppName('RatingsHistogram')
sc= SparkContext(conf=conf)

# Load Data
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

# Extract map the Data
ratings = lines.map(lambda x: x.split()[2])

result = ratings.countByValue()

# Sort and Display the result
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" %(key,value))