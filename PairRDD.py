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

print(pairs.top(5))

lenpairs = lines.map(lambda x: (x.split(" ")[0], len(x)))
print(lenpairs.take(5))

print("Pairs RDD Opprations")
print()
Datapairs = pairs.filter(lambda KeyValue : "Data" in KeyValue[1])
print(Datapairs.collect())

# Calculate the sum and count of values for each key using mapValues and reduceByKey

mydata = [("panda", 0),("pink",3),("pirate",3),("panda",1),("pink",4)]
pairRDD = sc.parallelize(mydata)
print(pairRDD.count())
print("pairRDD : ",pairRDD.collect())

mapValuesRDD = pairRDD.mapValues(lambda x:(x,1))
print("mapValuesRDD : ",mapValuesRDD.collect())

resultRDD = mapValuesRDD.reduceByKey(lambda x,y: (x[0]+y[0],(x[1]+y[1])))
print(resultRDD.count())
print("resultRDD : ",resultRDD.collect())

# Word count 
Words = lines.flatMap(lambda x:x.split(" "))
print(Words.take(5))

Result = Words.map(lambda x: (x,1)).reduceByKey(lambda x,y:x + y)
print(Result.count())
print(Result.take(5))



print('-'*50)