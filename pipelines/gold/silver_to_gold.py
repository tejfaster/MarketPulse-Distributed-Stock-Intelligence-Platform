import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import logging
import socket

# config
load_dotenv(os.path.join(os.path.dirname(__file__),'../../config/.env'))

# System
def get_base_path():
    hostname = socket.gethostname()
    if 'MacBook' in hostname:
        return '/Users/tejfaster/Developer/Python/MarketPulse-data'
    else:
        return '/home/tejfaster/MarketPulse-Data'

BASE_PATH = get_base_path()
SPARK_MASTER = 'spark://10.95.208.20:7077'
SILVER_PATH = f'{BASE_PATH}/silver/stocks'

# POSTGRES_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
POSTGRES_URL = f"jdbc:postgresql://10.95.208.20:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASS = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DRIVER = 'org.postgresql.Driver'

# logging basic config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger =logging.getLogger(__name__)


# SparkSession
def create_spark_session():
    return SparkSession.builder \
        .master(SPARK_MASTER).appName("MarketPulse-Gold") \
        .config('spark.pyspark.python', '/usr/bin/python3.11') \
        .config("spark.databricks.delta.logStore.relativePaths.enabled", "true") \
        .config('spark.executorEnv.DATA_BASE_PATH', '/home/tejfaster/MarketPulse-Data') \
        .config('spark.driverEnv.DATA_BASE_PATH', '/Users/tejfaster/Developer/Python/MarketPulse-data')\
        .config('spark.pyspark.driver.python', '/usr/bin/python3.11') \
        .config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.1.0')\
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')\
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')\
        .config('spark.databricks.delta.deltaLog.cacheSize',"0") \
        .getOrCreate()

# Postgres
def write_to_postgres(df,table_name):
    print(f"Writing{table_name} to postgressSQL...")
    df.write.format('jdbc') \
        .option('url',POSTGRES_URL) \
        .option('dbtable',table_name) \
        .option('user',POSTGRES_USER) \
        .option('password',POSTGRES_PASS) \
        .option('driver',POSTGRES_DRIVER) \
        .mode('overwrite') \
        .save()

def process_gold(spark):
    logger.info("Starting Gold Layer")
    logger.info(f"Reading from Gold: {SILVER_PATH}")

    df = spark.read.format("delta").load(SILVER_PATH).cache()
    logger.info(f"Total records: {df.count()}")

    # Stock_Price 
    stock_price = df.select(
        'symbol',
        'date',
        'price',
        'prev_close',
        'change',
        'change_pct',
        'volume',
        'timestamp'
    ).orderBy('symbol','date')

    # Technical_Signals
    technical_signals = df.select(
        'symbol',
        'date',
        'rsi',
        'macd',
        'macd_signal',
        'macd_histogram',
        'ema_12',
        'ema_26',
        'bb_upper',
        'bb_lower',
        'bb_width',
        'signal'
    ).orderBy('symbol','date')

    # Stock_Summary
    window = Window.partitionBy('symbol').orderBy(col('date').desc()) 

    stock_summary = df.withColumn('rn',row_number().over(window)) \
        .filter(col('rn') == 1) \
        .drop('rn') \
        .select(
            'symbol',
            'date',
            'price',
            'prev_close',
            'change',
            'change_pct',
            'volume',
            'rsi',
            'macd',
            'macd_signal',
            'bb_upper',
            'bb_lower',
            'bb_width',
            'signal'
        ).orderBy('symbol')
    
    write_to_postgres(stock_price,"gold.stock_prices")
    write_to_postgres(technical_signals,"gold.technical_signals")
    write_to_postgres(stock_summary,"gold.stock_summary")
    
    df.unpersist()


if __name__ == '__main__':
    try:
        spark = create_spark_session()
        process_gold(spark)
    except Exception as e:
        logger.error(f"Gold processing failed: {e}")
        raise
    finally:
        spark.stop()    
