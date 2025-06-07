from pyspark import SparkContext
from sys import stdin

#common lines
sc = SparkContext("local[*]","wordcount")

# setting log level information
sc.setLogLevel("ERROR")

# creating spark context
input = sc.textFile("D:/babul/Trendytech_datasets/search_data.txt")

#one input row will give multiple output rows
words = input.flatMap(lambda x: x.split(" "))

#one input row will give one output rows
word_Counts = words.map(lambda x : (x,1))

#take two rows and does aggregation and returns one row
final_Count = word_Counts.reduceByKey(lambda  x,y : x+y)

result = final_Count.collect()

for a in result:
     print(a)

# hold the program to see the DAG
#stdin.readline()