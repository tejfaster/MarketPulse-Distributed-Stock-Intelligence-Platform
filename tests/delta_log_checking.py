import os
from pyspark.sql import SparkSession
import json
from delta.tables import DeltaTable

BRONZE_PATH = '/Users/tejfaster/Developer/Python/MarketPulse-data/bronze/stocks'
log_path = f"{BRONZE_PATH}/_delta_log"

print("\n===== DEBUG DELTA LOG =====")

files = sorted(os.listdir(log_path))

print("Last 10 log files Spark sees:")
for f in files[-10:]:
    print(f)

print("===========================\n")

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("DeltaDebug") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load(BRONZE_PATH)

print("Delta table version loaded")
df.show(5)

with open(f"{BRONZE_PATH}/_delta_log/_last_checkpoint") as f:
    checkpoint = json.load(f)

print("Last checkpoint:", checkpoint)

dt = DeltaTable.forPath(spark,BRONZE_PATH)

history1 = dt.history(5)
history1.select("version","timestamp","operation").show(truncate=False)
print(df.rdd.getNumParttions())

log_path = f"{BRONZE_PATH}/_delta_log"

json_logs = [f for f in os.listdir(log_path) if f.endswith(".json")]

print("Total delta commits:", len(json_logs))
print("Latest commit:", sorted(json_logs)[-1])