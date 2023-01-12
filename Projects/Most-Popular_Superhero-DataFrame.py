from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder.appName("MostPopulerSuperhero").getOrCreate()

schema = StructType([\
                     StructField("id", 
IntegerType(), True),\
StructField("name",StringType(), True)])
    
name = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel-names.txt") 

lines = spark.read.text("file:///SparkCourse/Marvel-graph.txt")

connections = lines.withColumn("id",func.split(func.col("value"), " ") [0]).withColumn("connections", func.size(func.split(func.col("value"), " ")) -1).groupBy("id").agg(func.sum("connections").alias("connections"))

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopoulerName = name.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopoulerName[0] + " is the most popular super hero with " + str(mostPopular[1]) + " co-appearance")