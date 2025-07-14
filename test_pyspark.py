import os
os.environ["PYSPARK_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"

from pyspark import SparkContext ,SparkConf 

conf = SparkConf().setAppName("MyApp").setMaster("local")
sc = SparkContext(conf=conf)

lines = sc.textFile(r"D:\Programming\Data_Engineering\Apache_Spark\Spark_training\test_text.txt")

def HasPython(line):
    return "data" in line
Pythonlines = lines.filter(lambda line : HasPython(line))
print(Pythonlines.count())


for line in Pythonlines.collect():
    print(line)
    print("----")


print("Number of lines : " , lines.count())


sampleline = lines.sample(False, 0.3)
print("Number of sample lines " , sampleline.count()
      )