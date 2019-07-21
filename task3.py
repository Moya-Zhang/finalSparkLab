from pyspark import *
import os


def sumMapper(line):
    strIt = line.replace("(", "").replace("'", "").replace(")", "").split(", ")
    words = strIt[0].split(",")
    key = words[0]
    value = words[1]+","+strIt[1]
    return key, value


def normalizationMapper(args):
    if args[1].find(":") == -1:
        valueRes = args[1].split(",")[0]+","+"1.0"
    else:
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
    if valuesa.find(":") != -1:
        counta = int(valuesa.split(":")[0])
        valuesa = valuesa.split(":")[1]
    else:
        counta = int(valuesa.split(",")[1])
    if valuesb.find(":") != -1:
        countb = int(valuesb.split(":")[0])
        valuesb = valuesb.split(":")[1]
    else:
        countb = int(valuesb.split(",")[1])
    sum = counta+countb
    reduceRes += str(sum)
    return reduceRes+":"+valuesa+"|"+valuesb


if __name__ == '__main__':
    os.environ["SPARK_HOME"] = "D:\spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "D:\winutil"
    sc = SparkContext('local')
    # 从本地载入数据
    rdd = sc.textFile("./sparkTask2/part-00000")
    #with open("./sparkTask2/part-00000", 'r', encoding='utf-8') as out2:
    #    lines = out2.readlines()
    # Create RDD
    #rdd = sc.parallelize(rawData)
    # WordCount
    wordcount = rdd.map(sumMapper) \
        .reduceByKey(normalizationReducer)

    normalization = wordcount.map(normalizationMapper)
    normalization.sortBy(lambda x: x[0], False, 1).saveAsTextFile("./sparkTask3")


