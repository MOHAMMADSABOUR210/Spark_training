import os
os.environ["PYSPARK_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"

from pyspark import SparkContext ,SparkConf 

conf = SparkConf().setAppName("MyApp").setMaster("local")
sc = SparkContext(conf=conf)


list1 = ['coffee', 'coffee', 'panda', 'monkey', 'tea']
list2 = ['coffee', 'monkey', 'kitty']

RDD1 = sc.parallelize(list1)
RDD2 = sc.parallelize(list2)

# distinct
print("distinct : ")
DistinctRDD = RDD1.distinct()
print(DistinctRDD.collect())

# union
print("union : ")
unionRDD = RDD1.union(RDD2)
print(unionRDD.collect())

UDRDD = unionRDD.distinct()
print(UDRDD.collect())


# intersection
print("intersection : ")
intersectionRDD = RDD1.intersection(RDD2)
print(intersectionRDD.collect())


# subtract
print("subtract : ")
subtractRDD1 = RDD1.subtract(RDD2)
print(subtractRDD1.collect())


subtractRDD2 = RDD2.subtract(RDD1)
print(subtractRDD2.collect())