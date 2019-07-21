# coding:utf-8
# 在pyspark模块中引入SparkContext和SparkConf类
from pyspark import SparkContext, SparkConf
import os

os.environ['SPARK_HOME'] = r"D:\spark-2.4.3-bin-hadoop2.7"
os.environ['HADOOP_HOME'] = r'D:\winutil'

def calculate(x):
    # eg:<class 'tuple'>: ('丁同', (  (('李三', '0.125'),('李文秀', '0.625')), 1.0)  )
    res = list()
    val = float(x[1][1])
    endPoints = x[1][0]
    for end in endPoints:
        res.append(tuple((end[0], float(end[1]) * val)))
    return res

def graphBuilder(line):
    res = list()
    line = line.split()
    res.append(line[0])
    res.append(list())
    line[1] = line[1].split('|')
    for item in line[1]:
        item = item.split(',')
        res[1].append((item[0], item[1]))
    res[1] = tuple(res[1])
    return res


if __name__ == "__main__":
    #times为迭代次数
    times=10
    # 定义sparkContext
    sc = SparkContext('local', 'pr')

    # 得到原始数据并进行处理
    with open('out3.txt', 'r', encoding='utf-8') as out3:
        lines = out3.readlines()

    # 转换成key-values
    # tuple () 元组
    pages = sc.parallelize(lines).map(graphBuilder)

    # 初始pr值都设置为1
    links = pages.map(lambda x: (x[0], 1.0))

    # 按既定次数迭代
    for i in range(times):
        # join会把links和page按k合并，如('A',('D',))和('A',1.0) join之后变成 ('A', ('D',1.0))
        # flatMap调用了f函数，并把结果平铺
        rank = pages.join(links)
        rank=rank.flatMap(calculate)

        # reduce
        links = rank.reduceByKey(lambda x, y: x + y)

        # 修正
        links = links.mapValues(lambda x: 0.15 + 0.85 * x)

    links.sortBy(lambda x:x[1],False,1).saveAsTextFile("./sparkTask4")
    # j = links.collect()
    # for i in j:
    #     print(i)