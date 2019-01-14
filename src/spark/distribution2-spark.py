
from __future__ import print_function


from operator import add

from pyspark import SparkContext, SparkConf



if __name__ == "__main__":
    conf = SparkConf()
    conf.setMaster("spark://10.60.45.11:7078")
    conf.setAppName("distribution_system2")

    sc = SparkContext(conf=conf)
    readfile_rdd = sc.textFile("/distribution_system/tb_call_201202_random.txt")
    readfile_rdd = readfile_rdd.map(lambda x: x.split('\t'))
    type_opr_rdd = readfile_rdd.map(lambda x: (x[12],int(x[4])))


    def to_list(a):
        return [a]

    def append(a,b):
        a.append(b)
        return a

    def extend(a,b):
        a.extend(b)
        return a

    type_opr_rdd = type_opr_rdd.combineByKey(to_list,append,extend)
    type_opr_rdd_list = sorted(type_opr_rdd.collect())

    type1_rdd = sc.parallelize(type_opr_rdd_list[0][1])
    evtype1_rdd = type1_rdd.map(lambda x : (x,1))
    total_type1_rdd = evtype1_rdd.reduceByKey(add)
    total_type1_list = sorted(total_type1_rdd.collect())

    type2_rdd = sc.parallelize(type_opr_rdd_list[1][1])
    evtype2_rdd = type2_rdd.map(lambda x : (x, 1))
    total_type2_rdd = evtype2_rdd.reduceByKey(add)
    total_type2_list = sorted(total_type2_rdd.collect())

    type3_rdd = sc.parallelize(type_opr_rdd_list[2][1])
    evtype3_rdd = type3_rdd.map(lambda x : (x, 1))
    total_type3_rdd = evtype3_rdd.reduceByKey(add)
    total_type3_list = sorted(total_type3_rdd.collect())

    total_len1 = total_type1_list[0][1]+total_type1_list[1][1]+total_type1_list[2][1]
    ratio_opr1_1 = float(total_type1_list[0][1]) / float(total_len1)
    ratio_opr1_2 = float(total_type1_list[1][1]) / float(total_len1)
    ratio_opr1_3 = float(total_type1_list[2][1]) / float(total_len1)

    total_len2 = total_type2_list[0][1] +total_type2_list[1][1]+total_type2_list[2][1]
    ratio_opr2_1 = float(total_type2_list[0][1]) / float(total_len2)
    ratio_opr2_2 = float(total_type2_list[1][1]) / float(total_len2)
    ratio_opr2_3 = float(total_type2_list[2][1]) / float(total_len2)

    total_len3 = total_type3_list[0][1]+total_type3_list[1][1]+total_type3_list[2][1]
    ratio_opr3_1 = float(total_type3_list[0][1]) / float(total_len3)
    ratio_opr3_2 = float(total_type3_list[1][1]) / float(total_len3)
    ratio_opr3_3 = float(total_type3_list[2][1]) /float( total_len3)

    total_ratio = [(ratio_opr1_1,ratio_opr1_2,ratio_opr1_3),(ratio_opr2_1,ratio_opr2_2,ratio_opr2_3),(ratio_opr3_1,ratio_opr3_2,ratio_opr3_3)]

    with open('answer2.txt','w') as f1:
        for i in total_ratio :
            f1.write(str(i[0])+','+str(i[1])+','+str(i[2])+'\n')


