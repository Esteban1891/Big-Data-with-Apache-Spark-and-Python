from django import conf
from pyspark import SparkContext, SparkConf
import re
conf = SparkConf().setMaster("local").setAppName("Word Count")
sc = SparkContext(conf=conf)

def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


input = sc.textFile("/home/esteban/projects/pyspark/spark-programs/data/book.txt")
words = input.flatMap(normalize_words)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ':\t\t' + count)