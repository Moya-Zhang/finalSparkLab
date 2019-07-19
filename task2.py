from pyspark import *
import os


def cooccurrence(line):
    mapRes = []
    words = line.split(" ")
    for word1 in words:
        for word2 in words:
            if word1 != word2:
               mapRes.append(word1+","+word2)
    return mapRes


if __name__ == '__main__':
    os.environ["SPARK_HOME"] = "D:\spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "D:\winutil"
    sc = SparkContext('local')
    # 从本地模拟数据
    datas = ["you jump", "i jump", "i jump you"]
    # Create RDD
    rdd = sc.parallelize(datas)

    # WordCount
    wordcount = rdd.flatMap(cooccurrence)\
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    for wc in wordcount.collect():
        print(wc[0] + "\t" + str(wc[1]))
