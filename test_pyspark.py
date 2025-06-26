import os
os.environ["PYSPARK_PYTHON"] = r"C:\Program Files\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Program Files\Python310\python.exe"

from pyspark import SparkContext

sc = SparkContext(master="local", appName="MyApp")
lines = sc.textFile(r"C:\spark\README.md")


print(lines.count())
print(lines.first())

def HasPython(line):
    return "Python" in line

Pythonlines = lines.filter(lambda line : HasPython(line))
print(Pythonlines.count())
print(Pythonlines.first())

