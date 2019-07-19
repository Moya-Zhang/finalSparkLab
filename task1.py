from pyspark import *
import os


if __name__ == '__main__':
    os.environ["SPARK_HOME"] = "D:\spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "D:\winutil"
    sc = SparkContext('local')
    # 从本地模拟数据
    datas = ["you,jump", "i,jump"]
    # Create RDD
    rdd = sc.parallelize(datas)
    print(rdd.count())  # 2
    print(rdd.first())  # you,jum

    # WordCount
    wordcount = rdd.flatMap(lambda line: line.split(",")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    for wc in wordcount.collect():
        print(wc[0] + "\t" + str(wc[1]))




