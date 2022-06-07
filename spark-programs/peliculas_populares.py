from django import conf
from matplotlib import lines
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("movie_ratings")
sc = SparkContext(conf=conf)

lines = sc.textFile("/home/esteban/projects/pyspark/ml-100k/u.data")

movies = lines.map(lambda x: (int(x.split()[1]), 1))
moviescont = movies.reduceByKey(lambda x, y: x + y)
moviescontSorted = moviescont.map(lambda x: (x[1], x[0])).sortByKey()
results = moviescontSorted.collect()
for result in results:
    count = str(result[0])
    movie = result[1]
    print(movie, ':\t\t', count)