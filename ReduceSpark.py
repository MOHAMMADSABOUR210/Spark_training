import os
os.environ["PYSPARK_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"

from pyspark import SparkContext ,SparkConf 

conf = SparkConf().setAppName("MyApp").setMaster("local")
sc = SparkContext(conf=conf)



import random

random_list = [random.randint(1, 100) for _ in range(10)]
print(f"random_list: {random_list}")


RDD = sc.parallelize(random_list)
print(f"RDD: {RDD.collect()}")

sumRDD = RDD.reduce(lambda a, b: a + b)
print(f"Sum: {sumRDD}")

mulRDD = RDD.reduce(lambda a, b: a * b)
print(f"Mul: {mulRDD}")

maxRDD = RDD.reduce(lambda a, b: max(a, b))
print(f"Max: {maxRDD}")

minRDD = RDD.reduce(lambda a, b: min(a,b))
print(f"Min: {minRDD}")

averageRDD = sumRDD / RDD.count()
print(f"Average: {averageRDD}")



# aggregate()
sum_count = RDD.aggregate((0, 0),
                          lambda acc,value: (acc[0]+value, acc[1]+1),
                          lambda acc1, acc2 : (acc1[0]+acc2[0],acc1[1]+acc2[1]))

print( f"Average in aggregate: {sum_count[0]/ float(sum_count[1])}")
