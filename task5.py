# -*- coding: utf-8 -*-

from pyspark import *
import os


def f(x):
    # print(x)
    list1 = []
    s = len(x[1][0])
    for y in x[1][0]:
        list1.append(tuple((y[0], x[1][1] + "#" + x[0])))
    list1.append((x[0], x[1][0]))
    # print(list1)
    return list1


def read_file():
    graph = []
    name_list = []
    for line in open("./sparkTask3/part-00000", encoding='utf-8'):
        line = line[:-1]
        name = line.replace("(", "").replace(")", "").replace("'", "").split(", ")[0]
        neighbors = line.replace("(", "").replace(")", "").replace("'", "").split(", ")[1].split("|")
        n_list = []
        for neighbor in neighbors:
            n_list.append((neighbor.split(",")[0], neighbor.split(",")[1]))
        name_list.append(name)
        graph.append((name, tuple(n_list)))

    # print(name_list)
    # print(graph)
    return name_list, graph


def LPA_reducer(a, b):
    res_a = ""
    res_b = ""
    if isinstance(a, tuple):
        for item in a:
            res_a += (item[0] + "," + str(item[1]) + "|")
        res_a = res_a[:-1]
    else:
        res_a = a
    if isinstance(b, tuple):
        for item in b:
            res_b += (item[0] + "," + str(item[1]) + "|")
        res_b = res_b[:-1]
    else:
        res_b = b
    # print(res_a + ";" + res_b)
    return res_a + ";" + res_b


def update_label(args):
    # print("args", args)
    values = args.split(";")
    # print(values)
    name_label = {}
    label_weight = {}
    for value in values:
        if value.find("#") != -1:
            name_label[value.split("#")[1]] = value.split("#")[0]
        else:
            neighbors = value.split("|")

    for neighbor in neighbors:
        name = neighbor.split(",")[0]
        weight = float(neighbor.split(",")[1])
        label = name_label[name]
        if label in label_weight.keys():
            sum = label_weight[label]
            sum += weight
            label_weight[label] = sum
        else:
            label_weight[label] = weight

    max = -1
    max_label = ""
    for key in label_weight.keys():
        if label_weight[key] > max:
            max = label_weight[key]
            max_label = key

    return max_label


if __name__ == "__main__":
    os.environ["SPARK_HOME"] = "D:\spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "D:\winutil"

    # 定义sparkContext
    sc = SparkContext('local')

    # 原始数据
    # list = [('A', (('B', 0.7), ('C', 0.2), ('E', 0.1))), ('B', (('A', 0.5), ('C', 0.5))), ('C', (('B', 0.4), ('A', 0.6))), ('D', (('E', 1),)), ('E', (('D', 0.9), ('A', 0.1)))]
    name, list = read_file()

    # 必须转换成key-values,持久化操作提高效率，partitionBy将相同key的元素哈希到相同的机器上，
    # 省去了后续join操作shuffle开销
    # tuple () 元组
    pages = sc.parallelize(list).map(lambda x: (x[0], x[1])).partitionBy(4).cache()
    # print(pages)

    # 初始pr值都设置为1
    links = sc.parallelize(name).map(lambda x: (x, x))
    # print(links)

    # 开始迭代
    for i in range(1, 10):
        # join会把links和page按k合并，如('A',('D',))和('A',1.0) join之后变成 ('A', (('D',), 1.0))
        # flatMap调用了f函数，并把结果平铺
        rank = pages.join(links).flatMap(f)

        # reduce
        links = rank.reduceByKey(LPA_reducer)

        # 修正
        links = links.mapValues(update_label)

    # j = links.collect()
    #
    # for i in j:
    #     print(i)

    links = links.map(lambda x: (x[1], x[0])).reduceByKey(lambda x, y: x + "," + y)

    with open("task5", 'a') as file_object:
        for i in links.collect():
            file_object.write(i[0] + "\t" + i[1] + "\n")
