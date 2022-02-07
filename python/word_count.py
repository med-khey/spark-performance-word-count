from pyspark import SparkConf, SparkContext
import time


start_time = time.time()

conf = SparkConf()\
    .setAppName("Spark Performance Word Count")\
    .setMaster("local[1]")
sc = SparkContext(conf=conf)
text_file = sc.textFile(r"data\random_text.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)\
             .sortBy(lambda a: a[1],ascending=False)
counts.saveAsTextFile(r"data\output_python")

print( "Elapsed Time in mili seconds: " , (time.time() - start_time)*1000)
