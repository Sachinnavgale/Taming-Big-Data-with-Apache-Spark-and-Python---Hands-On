# Import necessary Lib
from pyspark import SparkConf, SparkContext

# set up Context
conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
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

# Filter out all the TMIN enteries
minTemp= parsedlines.filter(lambda x: "TMIN" in x [1])

# Create (StationID, Temperature) key/value pair
stationTemps = minTemp.map(lambda x: (x[0], x [2]))

# find minimum temperature by stationID
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))

# Collect and Display result
results= minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
     