import os
os.environ["PYSPARK_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"

from pyspark import SparkContext ,SparkConf 

conf = SparkConf().setAppName("MyApp").setMaster("local")
sc = SparkContext(conf=conf)

lines = sc.textFile(r"D:\Programming\Data_Engineering\Apache_Spark\Spark_training\test_text.txt")


DataAndEngineeringRDD = lines.filter(lambda line: "data" in line or "engineering" in line)
print(DataAndEngineeringRDD.count())

#  .take
for line in DataAndEngineeringRDD.take(10):
    print(line)
    print("----")


print("*****")
print(DataAndEngineeringRDD.take(10))


#  .collect

for line in DataAndEngineeringRDD.collect():
    print(line)
    print("----")   


print("*****")
print(DataAndEngineeringRDD.collect())

