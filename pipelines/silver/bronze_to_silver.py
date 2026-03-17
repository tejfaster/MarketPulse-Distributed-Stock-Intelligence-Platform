import logging
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, avg, stddev, lag, when, lit,
    round as spark_round, to_date, desc
)
from delta.tables import DeltaTable
import os
import re
import json

# Logging
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# config
SPARK_MASTER = 'local[*]'
BRONZE_PATH = '/Users/tejfaster/Developer/Python/MarketPulse-data/bronze/stocks'
SILVER_PATH = '/Users/tejfaster/Developer/Python/MarketPulse-data/silver/stocks'
CHECKPOINT_PATH = '/Users/tejfaster/Developer/Python/MarketPulse-data/checkpoints/silver'
SILVER_PROGRESS_FILE = "/Users/tejfaster/Developer/Python/MarketPulse-data/silver/.progress.json"

# SparkSession
def create_spark_session():
    return SparkSession.builder \
        .master(SPARK_MASTER) \
        .appName("MarketPulse-Silver") \
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.deltaLog.cacheSize", "0") \
        .getOrCreate()

# delta_log
def get_last_safe_version(bronze_path):
    log_path = os.path.join(bronze_path,"_delta_log")

    last_checkpoint_file = os.path.join(log_path, "_last_checkpoint")
    with open(last_checkpoint_file) as f:
        import json
        data = json.load(f)
    
    safe_version = data["version"]
    print(f"Using version from _last_checkpoint: {safe_version}")
    return safe_version

def get_last_processed_version():
    if os.path.exists(SILVER_PROGRESS_FILE):
        with open(SILVER_PROGRESS_FILE) as f:
            return json.load(f)["last_version"]
    return 0   

def save_progress(version):
    os.makedirs(os.path.dirname(SILVER_PROGRESS_FILE),exist_ok=True)
    with open(SILVER_PROGRESS_FILE, "w") as f:
        json.dump({"last_version":version},f) 

# RSI calcualtion
def calculate_rsi(df,period = 14):
    # window per sysmbol order by time
    window = Window.partitionBy("symbol").orderBy("timestamp")
    # getting price change from prevoius day
    df = df.withColumn(
        "price_change",
        col("price") - lag("price",1).over(window)
    )

    df = df.withColumn(
        "gain",
        when(col("price_change") > 0 ,
             col("price_change")).otherwise(0)
    )

    df = df.withColumn(
        "loss",
        when(col("price_change") < 0,
             -col("price_change")).otherwise(0)
    )

    window_14 = Window.partitionBy("symbol") \
        .orderBy("timestamp") \
        .rowsBetween(-period + 1,0)
    
    df = df.withColumn("avg_gain",
                       avg("gain").over(window_14))
    df = df.withColumn("avg_loss",
                       avg("loss").over(window_14))
    
    df = df.withColumn("rsi",
            when(col("avg_loss") == 0, lit(50.0))
            .otherwise(spark_round(100 - (
                 100 / (1 + (col("avg_gain") / col("avg_loss")))
            ),2) )         
    )

    return df

# MACD Calculation

def calculate_macd(df):
    window = Window.partitionBy("symbol").orderBy("timestamp")

    # EMA window
    window_12 = window.rowsBetween(-11,0)
    window_26 = window.rowsBetween(-25,0)
    window_9 = window.rowsBetween(-8,0)

    df = df.withColumn("ema_12",
                       avg("price").over(window_12))
    df = df.withColumn("ema_26",
                       avg("price").over(window_26))
    
    df = df.withColumn("macd",
            spark_round(col("ema_12") - col("ema_26"),4))
    
    df = df.withColumn("macd_signal",
            spark_round(avg("macd").over(window_9),4))
    
    df = df.withColumn("macd_histogram",
            spark_round(col("macd") - col("macd_signal"),4))
    
    return df 

def calculate_bollinger_bands(df, period = 20):
    window_20 = Window.partitionBy("symbol").orderBy("timestamp") \
    .rowsBetween(-period + 1,0)

    df = df.withColumn("bb_middle",
            spark_round(avg("price").over(window_20),4))
    
    df = df.withColumn("bb_std",
            stddev("price").over(window_20))
    
    df = df.withColumn("bb_upper",
            spark_round(
                col("bb_middle") + (2 * col("bb_std")) , 4
            ))
    
    df = df.withColumn("bb_lower",
            spark_round(
                col("bb_middle") - (2 * col("bb_std")) , 4
            ))
    
    df = df.withColumn("bb_width",
            spark_round(
                col("bb_upper") - col("bb_lower") , 4
            ))

    df = df.drop("bb_std")
    
    return df

def process_silver(spark):
    logger.info("Starting Silver layer Processing")
    logger.info(f"Reading from Bronze: {BRONZE_PATH}")

    last_processed = get_last_processed_version()
    safe_version = get_last_safe_version(BRONZE_PATH)

    if last_processed >= safe_version:
        print(f"Nothing new to proces. Bronze at v{safe_version},silver already at v{last_processed}")
        return
    
    print(f"Reading bronze v{last_processed} -> v{safe_version}")

    # df = spark.read.format("delta") \
    #     .option("versionAsOf",safe_version) \
    #     .load(BRONZE_PATH)
    df = spark.read.format("delta") \
        .option("versionAsOf",safe_version) \
        .load(BRONZE_PATH)

    logger.info(f"Total records: {df.count()}")
    logger.info(f"Symbols: {df.select('symbol').distinct().count()}")

    df = df.dropDuplicates(["symbol","timestamp"]) \
        .filter(col("price") > 0).orderBy("symbol","timestamp")
    
    logger.info("Calculating RSI...")
    df = calculate_rsi(df)

    logger.info("Calculating MACD...")
    df = calculate_macd(df)

    logger.info("Calculating Bollinger Bands...")
    df = calculate_bollinger_bands(df)

    df = df.withColumn("signal",
            when(col("rsi") > 70, "SELL")
            .when(col("rsi") < 30, "BUY")
            .otherwise("HOLD"))
    
    df = df.select(
        "symbol","price","prev_close",
        "change","change_pct","volume",
        "timestamp","date","rsi","macd",
        "macd_signal","macd_histogram",
        "ema_12","ema_26","bb_upper","bb_lower",
        "bb_width","signal"
    )

    logger.info(f"Writing to silver: {SILVER_PATH}")
    df.write \
        .format("delta").mode("overwrite") \
        .partitionBy("date","symbol") \
        .save(SILVER_PATH)
    
    logger.info(f"Silver layer complete!")
    logger.info(f"Records written: {df.count()}")
    df.select("symbol","price","rsi","macd","signal").show(10,truncate=False)

    save_progress(safe_version)
    print(f"Silver update.Progress saved at v{safe_version}")


if __name__ == '__main__':
    try:
        spark = create_spark_session()
        process_silver(spark)
    except Exception as e:
        logger.error(f"Silver processing failed: {e}")
        raise
    finally:
        spark.stop()