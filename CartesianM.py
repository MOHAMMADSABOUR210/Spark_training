import os
os.environ["PYSPARK_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"

from pyspark import SparkContext ,SparkConf 

conf = SparkConf().setAppName("MyApp").setMaster("local")
sc = SparkContext(conf=conf)


list1 = ['a', 'b', 'c']
list2 = [1,2,3,4,5]

RDD1 = sc.parallelize(list1)
RDD2 = sc.parallelize(list2)




print(RDD1.collect())
print()
print(RDD2.collect())
print()

cartRDD = RDD1.cartesian(RDD2)
print(cartRDD.collect())

print()
print(RDD1.count())
print(RDD2.count())
print(cartRDD.count())

