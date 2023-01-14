from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

schema = StructType([\
                     StructField("id", IntegerType(), True),\
                     StructField("name", StringType(), True)])
    
name = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel-names.txt")

lines = spark.read.text("file:///SparkCourse/Marvel-graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])\
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
minConnectionCount = connections.agg(func.min("connections")).first()[0]

minconnections = connections.filter(func.col("connections") == minConnectionCount)

minConnectionCountsWithnames = minconnections.join(name, "id")

print("the following characters have only" + str(minConnectionCount) + "connction(s):")

minConnectionCountsWithnames.select("name").show()