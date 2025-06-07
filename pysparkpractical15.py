#average movie rating example

from pyspark import SparkContext
from pyspark import StorageLevel
from sys import stdin

# creating Spark Context
sc = SparkContext("local[*]","Top Movie List")

# setting the logging level
sc.setLogLevel("ERROR")

# transformations
ratings = sc.textFile("D:/babul/sample_data/spark_dataset_for_practice/ratings.dat")

ratings_map_data = ratings.map(lambda x: (x.split("::")[1],float(x.split("::")[2])))

map_value_data = ratings_map_data.mapValues(lambda x: (x,1.0))

reduced_data = map_value_data.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

filter_data = reduced_data.filter(lambda x: x[1][1] >= 1000)

avg_rating = filter_data.map(lambda x: (x[0],x[1][0]/x[1][1]))

final_filter = avg_rating.filter(lambda x: x[1] >= 4.0)

# movie data
movies = sc.textFile("D:/babul/sample_data/spark_dataset_for_practice/movies.dat")

movie_map_data = movies.map(lambda x: (x.split("::")[0],x.split("::")[1]))

join_data = movie_map_data.join(final_filter)

top_movie_list = join_data.map(lambda x: x[1][0]).cache()

# action
results = top_movie_list.collect()
for x in results:
    print(x)

print(top_movie_list.count())

# hold the program to see DAG
#stdin.readline()
