# Import necessary Lib
from pyspark import SparkConf, SparkContext

# set up Context
conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

# Data Cleaning using def function
def parseline(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    
    return(stationID, entryType, temperature)

# Load Data
lines= sc.textFile("file:///SparkCourse/1800.csv")
parsedlines = lines.map(parseline)

# Filter out all the TMAX enteries
maxTemp= parsedlines.filter(lambda x: "TMAX" in x [1])

# Create (StationID, Temperature) key/value pair
stationTemps = maxTemp.map(lambda x: (x[0], x [2]))

# find minimum temperature by stationID
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))

# Collect and Display result
results= maxTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
     