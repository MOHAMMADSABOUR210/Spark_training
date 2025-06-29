import os
os.environ["PYSPARK_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"

from pyspark import SparkContext ,SparkConf 

conf = SparkConf().setAppName("MyApp").setMaster("local")\
    .set("spark.local.dir", "D:\Programming\Data_Engineering\Apache_Spark\spark_temp")

sc = SparkContext(conf=conf)

input = sc.textFile(r"C:\spark\README.md")
words = input.flatMap(lambda x: x.split())
word_counts = words.countByValue()

for word, count in word_counts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(f"{cleanWord.decode('utf-8')}: {count}")