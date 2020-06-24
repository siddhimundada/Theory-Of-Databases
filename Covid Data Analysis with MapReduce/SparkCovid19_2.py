from _future_ import print_function
import datetime
import sys
import csv
from operator import add

from pyspark.sql import SparkSession

def get_val(key, d, y):

    if key in d.keys():
        if(d[key]!=""):
            if(y!="0"):
                return float((float(y)/float(d[key]))*1000000.0)
    return 0


if _name_ == "_main_":

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    reader = csv.reader(open(sys.argv[2], 'r'))
    d = {}
    for row in reader:
        t1, t2, t3, t4, t5 = row
        if t5==None:
            d[t2] = 0
        else:
            d[t2] = t5

    d_broadcast = spark.sparkContext.broadcast(d)

    a = spark.read.csv(sys.argv[1], header = False)
    head = a.first()
    lines = a.rdd.filter(lambda x: x!=head)
    counts = lines.map(lambda x: (x[1],get_val(x[1],d_broadcast.value,x[2]))) \
                .reduceByKey(add) 

    output = counts.collect()
    for (word, count) in output:
        print("%s: %f" % (word, count))
    spark.stop()