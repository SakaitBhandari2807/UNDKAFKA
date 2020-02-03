from pyspark.sql.session import SparkSession

spark = SparkSession.builder.master("local").appName("First").getOrCreate()
spark.setLogLevel("INFO")
df = spark.read.option("header","true").option("inferSchema","true").csv("top50.csv")
df.take(3)



