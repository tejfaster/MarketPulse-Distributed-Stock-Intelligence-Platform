import socket
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://10.95.208.20:7077") \
    .appName("debug") \
    .config('spark.pyspark.python', '/usr/bin/python3.11') \
    .config('spark.pyspark.driver.python', '/usr/bin/python3.11') \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

def debug_task(partition):
    hostname = socket.gethostname()
    mac_path = '/Users/tejfaster/Developer/Python/MarketPulse-data/silver/stocks'
    win_path = '/home/tejfaster/MarketPulse-Data/silver/stocks'
    
    mac_exists = os.path.exists(mac_path)
    win_exists = os.path.exists(win_path)
    
    yield {
        'hostname': hostname,
        'mac_path_exists': mac_exists,
        'win_path_exists': win_exists,
    }

rdd = spark.sparkContext.parallelize([1, 2], 2)
results = rdd.mapPartitions(debug_task).collect()

for r in results:
    print(f"Machine: {r['hostname']}")
    print(f"  Mac path exists:     {r['mac_path_exists']}")
    print(f"  Windows path exists: {r['win_path_exists']}")
    print()

spark.stop()