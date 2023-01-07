# Import required lib
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("wordCount").getOrCreate()

#Read each line of the book into a dataframe
lines = spark.read.text("file:///SparkCourse/book.txt")

# Split using a regular expression that extracts words
words = lines.select(func.explode(func.split(lines.value, "\\W+")).alias("word"))
words.filter(words.word != "")

# Normalize everything to lowercase
lowercasewords = words.select(func.lower(words.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercasewords.groupBy("Word").count()

# Sort by counts
wordCountsShorted = wordCounts.sort("count")

# Show full results
wordCountsShorted.show(wordCountsShorted.count())
