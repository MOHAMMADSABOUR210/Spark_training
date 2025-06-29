import os
os.environ["PYSPARK_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"

from pyspark import SparkContext ,SparkConf 

conf = SparkConf().setAppName("MyApp").setMaster("local")
sc = SparkContext(conf=conf)

lines = sc.textFile(r"C:\spark\README.md")


print(lines.count())
print(lines.first())



print("way One : Use functions")
def HasPython(line):
    return "Python" in line
Pythonlines = lines.filter(lambda line : HasPython(line))
print(Pythonlines.count())
print(Pythonlines.first())


print("way two : in line functions")
Pythonlines = lines.filter(lambda line : "Python" in line)
Pythonlines.persist()
print(Pythonlines.count())
print(Pythonlines.first())


