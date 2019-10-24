#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
A K-means clustering program using MLlib.
requires pyspark 

This example requires NumPy (http://www.numpy.org/).
"""
from __future__ import print_function

import sys
import csv
import numpy as np
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans


def parseVector(line):
    l=line.split(',')

    if l[1]=='-':
        l[1]=0
    if l[2]=='-':
        l[2]=0
    return np.array([float(l[1]),float(l[2])])


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kmeans <file> <k>", file=sys.stderr)
        exit(-1)
    fp=open('ballout.csv','w')
    writer=csv.writer(fp)
    sc = SparkContext(appName="KMeans")
    lines = sc.textFile(sys.argv[1])
    data = lines.map(parseVector)
    k = int(sys.argv[2])
    model = KMeans.train(data, k)                               #batsman on ave and strik rate
    #model=KMeans.train(sc.parallelize(data),k,maxIterations=10,runs=30,initialzationMode="random")
    print("labels : " ,data.map(model.predict))                 #bowler on ave and no of wickets
    print("Final centers: " + str(model.clusterCenters))
    cluster_ind=model.predict(data)
    lis=[]
    f=open('bowl.csv','r')
    st=f.read()
    some=st.split('\n')
    i=0

    for x in cluster_ind.collect():
	print(x)
        l1=[]
        l1.append(x)
        split=some[i].split(',')
	split[2] = split[2][:-2]
	#print(split)
        l1.extend(split)
        i+=1
	#print(l1)
        lis.append(l1)

    lis=sorted(lis,key=lambda x: (x[0]),reverse=False)
    #print(lis)
    writer.writerows(lis)
    '''for s in lis:
	print(s)
        writer.writerow(s)'''
    print("Total Cost: " + str(model.computeCost(data)))
    #print("labels : " ,data.map(model.predict))
    sc.stop()
