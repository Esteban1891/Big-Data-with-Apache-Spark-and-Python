from unicodedata import name
from pyspark import SparkContext, SparkConf

def loadMoviesNames():
    movieNames = {}
    with open("/home/esteban/projects/pyspark/ml-100k/u.item", encoding='latin-1') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)

nameDict = sc.broadcast(loadMoviesNames())

lines = sc.textFile("/home/esteban/projects/pyspark/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda x: (nameDict.value[x[1]], x[0]))

results = sortedMoviesWithNames.collect()

for movie in results:
    print(movie)

