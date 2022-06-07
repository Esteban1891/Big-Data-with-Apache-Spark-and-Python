from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("AmigosPorEdad")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
def parse_line(line):
    fields = line.split(',')
    return (int(fields[2]), int(fields[3]))
"""
soy x (307, 1) soy y (253, 1)
soy x (323, 1) soy y (100, 1)
soy x (2, 1) soy y (281, 1)
soy x (191, 1) soy y (197, 1)
soy x (49, 1) soy y (249, 1)
soy x (181, 1) soy y (305, 1)
soy x (220, 1) soy y (167, 1)
"""
lines = sc.textFile("/home/esteban/projects/pyspark/spark-programs/data/amigos.csv")

rdd = lines.map(parse_line)

total_amigos = rdd.mapValues(lambda x: (x, 1))
total_amigos_edad = total_amigos.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
total_amigos_edad_prom = total_amigos_edad.mapValues(lambda x: x[0] / x[1])
print("Total amigos por edad: ", total_amigos_edad_prom.collect())
result = total_amigos_edad_prom.collect()
for i in result:
    print(i[0], ": ", i[1])