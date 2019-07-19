from pyspark import *
import os


def sumMapper(line):
    strIt = line.split("\t")
    words = strIt[0].split(",")
    key = words[0]
    value = words[1]+","+strIt[1]
    return key, value


def normalizationMapper(args):
    items = args[1].split(":")[1].split("|")
    sum = int(args[1].split(":")[0])
    names = []
    values = []
    valueRes = ""
    for item in items:
        if item != items[0]:
            valueRes += "|"
        names.append(item.split(",")[0])
        count = int(item.split(",")[1])
        values.append(count/sum)
        valueRes = valueRes + item.split(",")[0] + "," + str(count/sum)
    return args[0], valueRes


def normalizationReducer(valuesa,valuesb):
    reduceRes = ""
    counta = int(valuesa.split(",")[1])
    countb = int(valuesb.split(",")[1])
    if valuesa.find(":") != -1:
        counta = int(valuesa.split(":")[0])
        valuesa = valuesa.split(":")[1]
    if valuesb.find(":") != -1:
        countb = int(valuesb.split(":")[0])
        valuesb = valuesb.split(":")[1]
    sum = counta+countb
    reduceRes += str(sum)
    return reduceRes+":"+valuesa+"|"+valuesb


if __name__ == '__main__':
    os.environ["SPARK_HOME"] = "D:\spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "D:\winutil"
    sc = SparkContext('local')
    # 从本地模拟数据
    datas = ["you,jump	2"
             , "jump,you	2"
             , "i,jump	2"
             , "jump,i	2"
             , "i,you	1"
             , "you,i	1"]
    # Create RDD
    rdd = sc.parallelize(datas)

    # WordCount
    wordcount = rdd.map(sumMapper) \
        .reduceByKey(normalizationReducer)

    normalization = wordcount.map(normalizationMapper)
    for wc in normalization.collect():
        print(wc[0] + "\t" + str(wc[1]))


