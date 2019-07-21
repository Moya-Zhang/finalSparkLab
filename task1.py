from pyspark import *
import os
import jieba


def loadDic():
    nameList = []
    for line in open("./task2/people_name_list.txt", encoding='utf-8'):
        line = line[:-1]
        line.replace("\n", "")
        nameList.append(line)
        jieba.add_word(line)
    return nameList


nameList = loadDic()


def preprocessMapper(line):
    seg_list = list(jieba.cut(line))
    res = ""
    for each in seg_list:
        if each in nameList:
            if res.__len__() != 0:
                res += " "
            res += each
    if res.__len__() == 0:
        return "-1"
    return res


if __name__ == '__main__':
    os.environ["SPARK_HOME"] = "D:\spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "D:\winutil"
    sc = SparkContext('local')

    # 从本地读入数据
    rdd = sc.textFile("./task2/novels/*.txt")

    # WordCount
    word = rdd.map(preprocessMapper).filter(lambda x: "-1" not in x)
    word.saveAsTextFile("./sparkTask1")




