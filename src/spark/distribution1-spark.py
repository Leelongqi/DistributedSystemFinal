
from __future__ import print_function

#import sys
from operator import add

#from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


if __name__ == "__main__":
    conf = SparkConf()
    conf.setMaster("spark://10.60.45.11:7078")
    conf.setAppName("distribution_system1")

    sc = SparkContext(conf=conf)
    readfile_rdd = sc.textFile("/distribution_system/tb_call_201202_random.txt")
    readfile_rdd = readfile_rdd.map(lambda x: x.split('\t'))
    user_eytime_rdd = readfile_rdd.map(lambda x: (x[1],1))
    user_times_rdd = user_eytime_rdd.reduceByKey(add)



    date_rdd = readfile_rdd.map(lambda x: (x[1],int(x[0])))
    def to_list(a):
        return set([a])

    def append(a,b):
        a.add(b)
        return a

    def extend(a,b):
        a.union(b)
        return a

    date_rdd = date_rdd.combineByKey(to_list,append,extend)
    #print(date_rdd.collect()[0:10])
    date_times_rdd = date_rdd.map(lambda x:(x[0],len(x[1])))

    #date_times_list = date_times_rdd.collect()


    user_times_date_rdd = user_times_rdd.fullOuterJoin(date_times_rdd)
    user_times_date_rdd = user_times_date_rdd.map(lambda x :(x[0],float(x[1][0])/float(x[1][1])))
    user_avetd_list = user_times_date_rdd.collect()

    with open('answer1.txt', 'w') as f1:
        for i in user_avetd_list:
            f1.write(i[0]+','+str(i[1])+','+'\n')
