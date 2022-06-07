from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("TemperaturaMax")
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
maxTemp = parsedLine.filter(lambda x: x[1] == 'TMAX')
stationTemps = maxTemp.map(lambda x: (x[0], x[2]))
maxTemp = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = maxTemp.collect()
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))