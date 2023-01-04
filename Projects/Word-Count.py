# Import Recquired Lib
from pyspark import SparkConf, SparkContext
import re

# Set up Context
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# Load Data
input = sc.textFile("file:///SparkCourse/book.txt")

# Text Normalization
def normalization(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())
    
# Flatmap can create many new elements from each one
word = input.flatMap(normalization)

# Add up all values for each unique key!
wordCounts = word.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# Flip (Word, Count) = ( Count, Word) And sort by count as count i sour new key.
wordCountSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountSorted.collect()

# Clean and print the Data
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)