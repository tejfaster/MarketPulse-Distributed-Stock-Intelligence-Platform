from pyspark.sql import SparkSession

print("Starting Spark test...")

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("StackTest") \
    .config("spark.jars.packages","io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("Spark Version:", spark.version)

# create small dataframe
data = [("AAPL",150),("TSLA",200)]
df = spark.createDataFrame(data,["symbol","price"])

df.show()

# write delta test
path = "/tmp/delta_test"

df.write.format("delta").mode("overwrite").save(path)

print("Delta write OK")

# read delta
df2 = spark.read.format("delta").load(path)

print("Delta read OK")
df2.show()

spark.stop()

print("Everything working")