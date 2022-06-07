from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("TemperaturaMin")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

def parse_line(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0 #faregeing
    return (stationID, entryType, temperature)

lines = sc.textFile("/home/esteban/projects/pyspark/spark-programs/data/1800.csv")

parsedLine = lines.map(parse_line)
minTemps = parsedLine.filter(lambda x: x[1] == 'TMIN')
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect()
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))