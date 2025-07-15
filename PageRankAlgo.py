import os
os.environ["PYSPARK_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe"

from pyspark import SparkContext ,SparkConf 

conf = SparkConf().setAppName("MyApp").setMaster("local")
sc = SparkContext(conf=conf)

print('-'*50)

# Function to distribute the rank value from a page to its linked pages
def f(x):
    list1 = []
    s = len(x[1][0])  # Number of outgoing links from the current page
    for y in x[1][0]:  # For each linked page
        # Distribute the current page's rank equally among all its links
        list1.append(tuple((y, x[1][1] / s)))
    return list1

# List of pages and the pages they link to
list_pages = [('A', ('D',)), ('B', ('A',)), ('C', ('A', 'B')), ('D', ('A', 'C'))]

# Create an RDD for pages, partition it into 4, and cache it for reuse
pages = sc.parallelize(list_pages).map(lambda x: (x[0], tuple(x[1])))\
        .partitionBy(4).cache()

# Initialize ranks of all pages to 1.0
links = sc.parallelize(['A', 'B', 'C', 'D']).map(lambda x: (x, 1.0))

# Iteratively compute PageRank for 9 iterations
for i in range(1, 10):

    # Join structure: (page, (links, current rank)), then distribute rank
    rank = pages.join(links).flatMap(f)

    # Sum up ranks received by each page
    links = rank.reduceByKey(lambda x, y: x + y)

    # Apply damping factor to compute final rank
    links = links.mapValues(lambda x: 0.15 + 0.85 * x)

# Print final ranks of all pages
for i in links.collect():
    print(i)

print('-'*50)
