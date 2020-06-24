from _future_ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession


if _name_ == "_main_":
    # if len(sys.argv) != 2:
    #     print("Usage: wordcount <file>", file=sys.stderr)
    #     sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    # lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    lines = spark.read.csv(sys.argv[1]).rdd.map(lambda r: r)
    lines1 = lines.filter(lambda l: "location" not in l)
    start_data=sys.argv[2]
    end_data=sys.argv[3]

    counts = lines1.filter(lambda a: a[0]>=start_data and a[0]<=end_data) \
                .map(lambda a:(a[1],int(a[3]))) \
                .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()