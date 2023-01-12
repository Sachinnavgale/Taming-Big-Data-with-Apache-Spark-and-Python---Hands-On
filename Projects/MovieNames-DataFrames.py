from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

def loadMovieNames():
    movieNames = {}
    with codecs.open ("C:/SparkCourse/ml-100k/u.ITEM", "r", encoding = "ISO-8859-1", errors = 'ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
        return movieNames
    
spark = SparkSession.builder.appName("PopularMovie").getOrCreate()
    
nameDict = spark.sparkContext.broadcast(loadMovieNames())
                                           
# Create schema when reading u.data
schema = StructType([\
                     StructField("userID", IntegerType(), True),\
                     StructField("movieID", IntegerType(), True),\
                     StructField("rating", IntegerType(), True),\
                     StructField("tiestamp", LongType(), True),])


# Load Movie Data as DataFrame
movieDF = spark.read.option("sep", "\t").schema(schema).csv("file:///SparkCourse/ml-100k/u.data")

moviecounts = movieDF.groupBy("movieID").count()


# Create a user Defined Functions to look up movie names from our brodcasted dictionary
def loockupName(movieID):
    return nameDict.value[movieID]

lookupNameUDF = func.udf(loockupName)


# Add a movie Title column using our new udf
moviewithnames = moviecounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))


# Sort the results
sortedMoviewithNames = moviewithnames.orderBy(func.desc("count"))

# Group the toip 10
sortedMoviewithNames.show(10, False)

# Stop the session
spark.stop()