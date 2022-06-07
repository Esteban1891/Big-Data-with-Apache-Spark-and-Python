from pyspark import SparkContext, SparkConf
import collections

conf = SparkConf().setMaster("local").setAppName("Contador de ratings")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")
lines = sc.textFile("/home/esteban/projects/pyspark/ml-100k/u.data")

ratings = lines.map(lambda x: x.split()[2])

result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))

for key, value in sortedResults.items():
    print("%s %i" % (key, value))