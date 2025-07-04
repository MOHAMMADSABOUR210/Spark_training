import os
os.environ["PYSPARK_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"

from pyspark import SparkContext ,SparkConf 

conf = SparkConf().setAppName("MyApp").setMaster("local")
sc = SparkContext(conf=conf)


list1 = [1,2,3,4,5]

inputRDD = sc.parallelize(list1)
print(inputRDD.collect())

filterRDD = inputRDD.filter(lambda num : num != 1)
print(filterRDD.collect())


# map
mapRDD = inputRDD.map(lambda num : num ** 2)
print(mapRDD.collect())


# flatmap
words = ["Hello Apache Spark", "DATA_ENGITNEER"]
lineRDD = sc.parallelize(words)
print(lineRDD)


flatRDD = lineRDD.flatMap(lambda st:st.split(" "))
print(flatRDD.collect())

