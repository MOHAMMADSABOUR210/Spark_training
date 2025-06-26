import os
os.environ["PYSPARK_PYTHON"] = r"C:\Program Files\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Program Files\Python310\python.exe"

from pyspark import SparkContext

sc = SparkContext(master="local", appName="MyApp")

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
print(Pythonlines.count())
print(Pythonlines.first())


