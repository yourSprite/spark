#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@File Name: wordCount.py
@Create Time: 2019-06-29 23:56
@Author: wangyutian
@Version: 1.0
@Python Version: Python 3.6.4
@Modify Time: 2019-06-29 23:56
'''

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('wordCount').setMaster('local[*]')
sc = SparkContext(conf=conf)
# inputFile = "hdfs://192.168.199.106:9000/user/hadoop/input/"
inputFile = "file:///Users/wangyutian/code/wordcount.txt"
outputFile = "file:///Users/wangyutian/code"
textFile = sc.textFile(inputFile)
word2Count = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
word2Count.foreach(print)
