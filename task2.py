from pyspark import *
import os


def cooccurrence(line):
    mapRes = []
    words = line.split(" ")
    words = set(words)
    for word1 in words:
        for word2 in words:
            if word1 != word2:
               mapRes.append(word1+","+word2)
    return mapRes
#提取文件夹下的地址+文件名，源文件设定排序规则

def load_data(file_dir):
    L = []
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            if os.path.splitext(file)[1] != '.crc':
                L.append(os.path.join(root , file))
    datas = []
    for filename in L:
        for line in open(filename, encoding='utf-8'):
            line = line[:-1]
            datas.append(line)
    return datas


if __name__ == '__main__':
    os.environ["SPARK_HOME"] = "D:\spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "D:\winutil"
    sc = SparkContext('local')
    # 从本地模拟数据
    #datas = ["you jump jump", "i jump", "i jump you"]

    # Create RDD
    datas = load_data("./sparkTask1/")
    rdd = sc.parallelize(datas)
    # WordCount
    wordcount = rdd.flatMap(cooccurrence)\
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    wordcount.sortBy(lambda x: x[0], False, 1).saveAsTextFile("./sparkTask2")
