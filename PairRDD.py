import os
os.environ["PYSPARK_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"

from pyspark import SparkContext ,SparkConf 

conf = SparkConf().setAppName("MyApp").setMaster("local")
sc = SparkContext(conf=conf)

lines = sc.textFile(r"D:\Programming\Data_Engineering\Apache_Spark\Spark_training\test_text.txt")

print('-'*50)

print(lines.first())

pairs = lines.map(lambda x: (x.split(" ")[0], x))
print(pairs.count())
print(pairs.first())

print(pairs.top(10))

lenpairs = lines.map(lambda x: (x.split(" ")[0], len(x)))
print(lenpairs.take(5))


print('-'*50)
