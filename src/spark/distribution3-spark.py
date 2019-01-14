# -*- coding: utf-8 -*-
from __future__ import print_function

#from operator import add

from pyspark import SparkContext, SparkConf


def tran_s(x):
    h, m, s = x[9].split(":")
    st_sec = int(h) * 3600 + int(m) * 60 + int(s)
    return (x[1], [st_sec, int(x[11])])


def start_time(x):
    """
    :param x[1][0]: 开始时间
    :param x[0]: 用户号码
    :param x[1][1]: 通话持续时间
    :return: 列表更新
    """
    start_t = x[1][0]
    user = x[0]
    dutime = x[1][1]
    idx = int(start_t / (3 * 3600))  # 第几个时间段
    v = (user, [0]*8)
    # v = ndict[user] if user in ndict else value()
    if start_t + dutime <= (idx + 1) * 3 * 3600:
        v[1][idx] += dutime
    else:
        v[1][idx] += ((idx + 1) * 3 * 3600) - start_t
        v[1][(idx + 1) % 8] += start_t + dutime - ((idx + 1) * 3 * 3600)
    return v

def sect(x):
    sum_list=[0]*8
    for t in x[1]:
        for t1 in range(len(t)):
            sum_list[t1] += t[t1]
    return (x[0],sum_list)


if __name__ == "__main__":
    conf = SparkConf()
    conf.setMaster("spark://10.60.45.11:7078")
    conf.setAppName("distribution_system3")

    sc = SparkContext(conf=conf)
    readfile_rdd = sc.textFile("/distribution_system/tb_call_201202_random.txt")
    readfile_rdd = readfile_rdd.map(lambda x: x.split('\t'))
    user_dur_rdd = readfile_rdd.map(tran_s)
    user_dur_rdd = user_dur_rdd.map(start_time)

    def to_list(a):
        return [a]

    def append(a,b):
        a.append(b)
        return a

    def extend(a,b):
        a.extend(b)
        return a

    user_dur_rdd = user_dur_rdd.combineByKey(to_list,append,extend)
    user_dur_list=user_dur_rdd.map(sect).collect()


    with open('answer3.txt', 'w') as f1:
        for i in user_dur_list:
            sm = float(sum(i[1]))
            if not sm == 0:
                ratio_t = [str(float(t)/sm) for t in i[1]]
                s_ratio_t = ",".join(ratio_t)
                f1.write( i[0]+ ',' + s_ratio_t + ',' + '\n')
